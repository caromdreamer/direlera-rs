// Session-based UDP handling - TCP-like session management for UDP
// Each client gets its own session handler, similar to TCP connections

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tracing::{debug, info, warn, Instrument};

use crate::{fields, packet_util, AppState};

/// Configuration for session timeout behavior
const SESSION_TIMEOUT: Duration = Duration::from_secs(120);
/// Grace period for a session that has not yet logged in. Any game-port packet
/// from a new addr spawns a session so the stateful login handshake (USER_LOGIN
/// -> ACK round trips) has somewhere to run, but a connection that never
/// completes login (server-browser probes, stray/out-of-sequence packets,
/// rejected logins) is not a real user. It's reaped quickly and silently instead
/// of squatting a full SESSION_TIMEOUT and emitting a spurious lobby notice.
const PRE_LOGIN_TIMEOUT: Duration = Duration::from_secs(15);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(3);
const LOGIN_ACK_RESEND_INTERVAL: Duration = Duration::from_millis(100);

/// Stall recovery: if a playing client hasn't sent game input for this long, it
/// likely missed a server->client packet (lockstep freeze). Resend its last
/// packet so it can catch up. Checked every STALL_RESEND_INTERVAL.
const STALL_THRESHOLD: Duration = Duration::from_millis(100);
const STALL_RESEND_INTERVAL: Duration = Duration::from_millis(50);

/// During a PLAYING game a client streams input continuously, so going silent
/// for this long means it's gone (process killed / hard crash) rather than a
/// transient network stall. Reap it instead of waiting out the 120s keepalive —
/// in lockstep a missing player freezes the game for everyone else.
///
/// Set well above any plausible network blip: until this fires, the stall-resend
/// keeps poking the client every STALL_RESEND_INTERVAL, so a connection that
/// recovers within the window catches up and the game continues. Past it, the
/// player loses their slot (and, if they were the owner, the room closes). Only
/// applies while playing; an idle client in the lobby keeps the full SESSION_TIMEOUT.
const PLAYING_INPUT_TIMEOUT: Duration = Duration::from_secs(15);

/// Represents a single UDP "session" - simulating TCP connection
struct UdpSession {
    last_seen: Instant,
    tx: mpsc::Sender<Vec<u8>>,
}

/// Manages all active UDP sessions
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<SocketAddr, UdpSession>>>,
    packet_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
    /// Signals that a session should be torn down immediately (e.g. the client
    /// quit normally). Removing the `sessions` entry drops the last `tx`, so the
    /// session task's `rx.recv()` returns `None` and it ends gracefully instead
    /// of waiting out SESSION_TIMEOUT.
    session_close_tx: mpsc::Sender<SocketAddr>,
}

impl SessionManager {
    #[allow(clippy::type_complexity)]
    pub fn new() -> (
        Self,
        mpsc::Receiver<(SocketAddr, Vec<u8>)>,
        mpsc::Receiver<SocketAddr>,
    ) {
        let (packet_tx, packet_rx) = mpsc::channel(1000);
        let (session_close_tx, session_close_rx) = mpsc::channel(1000);

        let manager = Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            packet_tx,
            session_close_tx,
        };

        (manager, packet_rx, session_close_rx)
    }

    /// Get the sender for the main UDP dispatcher to send packets
    pub fn packet_sender(&self) -> mpsc::Sender<(SocketAddr, Vec<u8>)> {
        self.packet_tx.clone()
    }

    /// Get the sender used to request immediate teardown of a session.
    pub fn session_close_sender(&self) -> mpsc::Sender<SocketAddr> {
        self.session_close_tx.clone()
    }

    /// Start the session manager - spawns session handlers as needed
    pub async fn run(
        self: Arc<Self>,
        mut packet_rx: mpsc::Receiver<(SocketAddr, Vec<u8>)>,
        mut session_close_rx: mpsc::Receiver<SocketAddr>,
        global_state: Arc<AppState>,
    ) {
        info!("Session manager started");

        loop {
            tokio::select! {
                maybe_packet = packet_rx.recv() => {
                    let Some((addr, data)) = maybe_packet else { break };
                    let tx = {
                        let sessions = self.sessions.read().await;
                        sessions.get(&addr).map(|session| session.tx.clone())
                    };

                    if let Some(tx) = tx {
                        // Existing session - forward packet
                        if let Err(e) = tx.send(data).await {
                            warn!(
                                { fields::ADDR } = %addr,
                                { fields::ERROR } = %e,
                                "Failed to forward packet to session"
                            );
                        }
                    } else {
                        // New session - spawn handler
                        self.spawn_session_handler(addr, data, global_state.clone())
                            .await;
                    }
                }
                maybe_close = session_close_rx.recv() => {
                    let Some(addr) = maybe_close else { break };
                    // Drop the session entry: this drops the last `tx`, so the
                    // session task's `rx.recv()` returns `None` and it shuts down
                    // gracefully (Ok(None) branch) instead of waiting out the
                    // full SESSION_TIMEOUT and emitting a spurious timeout notice.
                    let removed = { self.sessions.write().await.remove(&addr) };
                    if removed.is_some() {
                        debug!({ fields::ADDR } = %addr, "Session closed on request");
                    }
                }
            }
        }
    }

    /// Spawn a new session handler for a client address
    async fn spawn_session_handler(
        &self,
        addr: SocketAddr,
        initial_data: Vec<u8>,
        global_state: Arc<AppState>,
    ) {
        let (tx, rx) = mpsc::channel(100);

        let session = UdpSession {
            last_seen: Instant::now(),
            tx: tx.clone(),
        };

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(addr, session);
        }

        debug!({ fields::ADDR } = %addr, "New session spawned");

        // Send initial packet to the session channel
        if let Err(e) = tx.send(initial_data).await {
            warn!(
                { fields::ADDR } = %addr,
                { fields::ERROR } = %e,
                "Failed to send initial packet to session"
            );
        }

        // Spawn session handler task
        let sessions = self.sessions.clone();
        tokio::spawn(async move {
            handle_session(addr, rx, sessions, global_state).await;
        });
    }

    /// Start the periodic cleanup task
    pub fn start_cleanup_task(self: Arc<Self>, global_state: Arc<AppState>) {
        tokio::spawn(async move {
            info!("Session cleanup task started");

            loop {
                tokio::time::sleep(CLEANUP_INTERVAL).await;

                let now = Instant::now();
                let mut sessions = self.sessions.write().await;

                let mut to_remove = Vec::new();

                for (addr, session) in sessions.iter() {
                    let elapsed = now.duration_since(session.last_seen);
                    if elapsed > SESSION_TIMEOUT {
                        info!(
                            { fields::ADDR } = %addr,
                            inactive_duration = ?elapsed,
                            "Session timeout"
                        );
                        to_remove.push(*addr);
                    }
                }

                for addr in &to_remove {
                    sessions.remove(addr);
                }

                drop(sessions);

                // Clean up from global state and notify lobby
                for addr in to_remove {
                    if let Some(client_info) = global_state.get_client(&addr).await {
                        // If the client was in a game, perform quit game flow
                        if client_info.game_id.is_some() {
                            let _ = crate::handlers::game::handle_quit_game(
                                vec![0x00, 0xFF, 0xFF],
                                &addr,
                                global_state.clone(),
                            )
                            .await;
                        }

                        // Remove client and broadcast USER_QUIT to lobby
                        if let Some(removed) = global_state.remove_client(&addr).await {
                            use crate::kaillera::message_types as msg;
                            let data = packet_util::build_user_quit_packet(
                                &removed.username,
                                removed.user_id,
                                b"timeout",
                            );
                            let _ = crate::handlers::util::broadcast_packet(
                                &global_state,
                                msg::USER_QUIT,
                                data,
                            )
                            .await;
                        }
                    } else {
                        // Fallback: ensure removal
                        let _ = global_state.remove_client(&addr).await;
                    }
                }
            }
        });
    }
}

/// Periodically resend the last packet to any playing client that has stalled
/// (stopped sending game input). In lockstep, a client that misses a server->client
/// combined frame stops advancing and stops sending input, deadlocking the game.
/// Resending the last packet (verbatim, same message numbers) lets it recover;
/// already-processed messages are deduped by the client.
pub fn start_stall_resend_task(global_state: Arc<AppState>) {
    tokio::spawn(async move {
        info!("Stall resend task started");
        loop {
            tokio::time::sleep(STALL_RESEND_INTERVAL).await;
            let now = Instant::now();

            // Collect, per playing game, players that stalled (need a resend) and
            // players that have gone silent long enough to be considered gone (reap).
            type StalledPlayer = (SocketAddr, Arc<crate::state::GameMetricLabels>, Duration);
            let (stalled, dead): (Vec<StalledPlayer>, Vec<SocketAddr>) = {
                let game_arcs: Vec<_> =
                    { global_state.games.read().await.values().cloned().collect() };
                let mut out = Vec::new();
                let mut dead = Vec::new();
                for game_arc in game_arcs {
                    let game = game_arc.lock().await;
                    if game.game_status != crate::state::GAME_STATUS_PLAYING {
                        continue;
                    }
                    for (player_id, p) in game.players.iter().enumerate() {
                        // The game stays PLAYING until the last player leaves, so a
                        // player who already dropped is still listed here with a stale
                        // last_game_data_recv. They aren't waiting on input — resending
                        // to them is wasted work and floods the log until they fully quit.
                        if game
                            .sync_manager
                            .as_ref()
                            .is_some_and(|m| m.is_player_dropped(player_id))
                        {
                            continue;
                        }
                        if let Some(last) = p.last_game_data_recv {
                            let stalled_for = now.duration_since(last);
                            if stalled_for >= PLAYING_INPUT_TIMEOUT {
                                // Gone, not merely stalled: reap instead of resending.
                                dead.push(p.addr);
                            } else if stalled_for >= STALL_THRESHOLD {
                                out.push((p.addr, game.metric_labels.clone(), stalled_for));
                            }
                        }
                    }
                }
                (out, dead)
            };

            // How many players are stalled right now (across all playing games).
            metrics::gauge!("stalled_players").set(stalled.len() as f64);

            // Resend each stalled client's last packet verbatim (same msg numbers).
            for (addr, labels, stalled_for) in stalled {
                let last = {
                    let addr_map = global_state.clients_by_addr.read().await;
                    let id_map = global_state.clients_by_id.read().await;
                    addr_map
                        .get(&addr)
                        .and_then(|sid| id_map.get(sid))
                        .and_then(|c| c.packet_generator.last_sent())
                };
                if let Some(data) = last {
                    let _ = global_state.tx.send(crate::Message { data, addr }).await;
                    // Stall detection #2 (push / outbound): the server noticed the client
                    // stopped sending input for >=100ms and proactively resends its last
                    // packet. Compare this timestamp against the zero-new-inbound log to
                    // see which detector fires first.
                    info!(
                        detector = "stall-resend-push",
                        { fields::ADDR } = %addr,
                        stalled_ms = stalled_for.as_millis() as u64,
                        "stall detected: resending last packet to stalled client"
                    );
                    // game_name/emulator_name (not the per-instance game_uid) keep
                    // this counter's cardinality bounded by the real game catalog,
                    // so it needs no idle-expiry like the histogram series do.
                    metrics::counter!(
                        "stall_resends_total",
                        "game_name" => labels.game_name.clone(),
                        "emulator_name" => labels.emulator_name.clone(),
                    )
                    .increment(1);
                }
            }

            // Reap players that have sent no input for PLAYING_INPUT_TIMEOUT: in a
            // lockstep game this is a dead client (process killed), not a stall, and
            // it freezes the game for everyone else until the 120s keepalive fires.
            // Request session teardown; the session task's graceful close runs the
            // full quit flow (closes the game if this was the owner, releases the
            // co-players). Idempotent — a duplicate close before teardown is a no-op.
            for addr in dead {
                warn!(
                    { fields::ADDR } = %addr,
                    timeout = ?PLAYING_INPUT_TIMEOUT,
                    "Playing client sent no game input past timeout; requesting session teardown"
                );
                let _ = global_state.session_close_tx.send(addr).await;
            }
        }
    });
}

/// Keep the login ACK handshake reliable over UDP. A client that misses the
/// first SERVER_TO_CLIENT_ACK has no game/lobby state yet, so the gameplay stall
/// resend task cannot help it. While the ACK speed-test is incomplete, resend the
/// last outbound packet verbatim; clients dedupe by message number.
pub fn start_login_resend_task(global_state: Arc<AppState>) {
    tokio::spawn(async move {
        debug!("Login ACK resend task started");
        loop {
            tokio::time::sleep(LOGIN_ACK_RESEND_INTERVAL).await;

            let pending: Vec<(SocketAddr, Vec<u8>)> = {
                let addr_map = global_state.clients_by_addr.read().await;
                let id_map = global_state.clients_by_id.read().await;
                addr_map
                    .iter()
                    .filter_map(|(addr, sid)| {
                        let client = id_map.get(sid)?;
                        if client.game_id.is_none()
                            && client.ack_count <= crate::handlers::NUM_ACKS_FOR_SPEED_TEST
                        {
                            client
                                .packet_generator
                                .last_sent()
                                .map(|packet| (*addr, packet))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            for (addr, data) in pending {
                let _ = global_state.tx.send(crate::Message { data, addr }).await;
            }
        }
    });
}

/// Handle a single session - this is like handling a TCP connection
async fn handle_session(
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Vec<u8>>,
    sessions: Arc<RwLock<HashMap<SocketAddr, UdpSession>>>,
    global_state: Arc<AppState>,
) {
    // Long-lived per-session span. Identity/ping/game_id are recorded onto it as
    // they become known or change (login, ACK, join/quit), and every child
    // handler span inherits them — so handlers no longer re-stamp these fields.
    let span = tracing::info_span!(
        "session",
        addr = %addr,
        user_name = tracing::field::Empty,
        user_id = tracing::field::Empty,
        connection_type = tracing::field::Empty,
        ping = tracing::field::Empty,
        session_id = tracing::field::Empty,
        game_id = tracing::field::Empty,
    );
    async move {
        debug!("Session handler started");

        // Per-session packet counter — no global lock needed
        let mut packet_counter: u16 = 0;
        let mut pending_messages = BTreeMap::new();

        // Last-known identity (log-friendly name + user_id), cached once the
        // client logs in. If the client is already gone from global state by the
        // time a timeout fires (e.g. cleanup_task won the race), this still lets
        // the timeout notice name who dropped instead of falling back to ip:port.
        let mut cached_identity: Option<(String, u16)> = None;

        // Session loop - similar to TCP recv loop
        loop {
            // A session that hasn't logged in yet (no cached identity) only gets a
            // short grace window; once logged in it gets the full keepalive timeout.
            let recv_timeout = if cached_identity.is_some() {
                SESSION_TIMEOUT
            } else {
                PRE_LOGIN_TIMEOUT
            };
            match timeout(recv_timeout, rx.recv()).await {
                Ok(Some(data)) => {
                    // Update last_seen
                    {
                        let mut sessions_lock = sessions.write().await;
                        if let Some(session) = sessions_lock.get_mut(&addr) {
                            session.last_seen = Instant::now();
                        }
                    }

                    global_state.update_client_activity(&addr).await;
                    crate::process_packet_in_session(
                        data,
                        addr,
                        global_state.clone(),
                        &mut packet_counter,
                        &mut pending_messages,
                    )
                    .await;

                    // Cache identity once it exists (cheap: only until populated,
                    // so the hot game-input path skips the lookup afterward).
                    if cached_identity.is_none() {
                        if let Some(client_info) = global_state.get_client(&addr).await {
                            let name = crate::handlers::util::bytes_for_log(&client_info.username);
                            cached_identity = Some((name, client_info.user_id));
                        }
                    }
                }
                Ok(None) => {
                    // Notify lobby and perform quit if necessary before breaking
                    if let Some(client_info) = global_state.get_client(&addr).await {
                        if client_info.game_id.is_some() {
                            let _ = crate::handlers::game::handle_quit_game(
                                vec![0x00, 0xFF, 0xFF],
                                &addr,
                                global_state.clone(),
                            )
                            .await;
                        }
                        if let Some(removed) = global_state.remove_client(&addr).await {
                            use crate::kaillera::message_types as msg;
                            let data = packet_util::build_user_quit_packet(
                                &removed.username,
                                removed.user_id,
                                b"disconnected",
                            );
                            let _ = crate::handlers::util::broadcast_packet(
                                &global_state,
                                msg::USER_QUIT,
                                data,
                            )
                            .await;
                        }
                    }
                    break;
                }
                Err(_) => {
                    warn!(timeout_duration = ?recv_timeout, "Session timeout");
                    use crate::kaillera::message_types as msg;
                    if let Some(client_info) = global_state.get_client(&addr).await {
                        let username = crate::handlers::util::bytes_for_log(&client_info.username);
                        // 글로벌 채팅으로 타임아웃 알림 (디버깅용)
                        // 닉네임 + user_id + ip:port 까지 실어 누가 끊겼는지 식별 가능하게.
                        let notice = format!(
                            "[Server] {} (#{}, {}) timed out (keepalive not received)",
                            username, client_info.user_id, addr
                        );
                        let data =
                            packet_util::build_global_chat_packet(b"Server", notice.as_bytes());
                        let _ = crate::handlers::util::broadcast_packet(
                            &global_state,
                            msg::GLOBAL_CHAT,
                            data,
                        )
                        .await;

                        if client_info.game_id.is_some() {
                            let _ = crate::handlers::game::handle_quit_game(
                                vec![0x00, 0xFF, 0xFF],
                                &addr,
                                global_state.clone(),
                            )
                            .await;
                        }
                        if let Some(removed) = global_state.remove_client(&addr).await {
                            let data = packet_util::build_user_quit_packet(
                                &removed.username,
                                removed.user_id,
                                b"timeout",
                            );
                            let _ = crate::handlers::util::broadcast_packet(
                                &global_state,
                                msg::USER_QUIT,
                                data,
                            )
                            .await;
                        }
                    } else if let Some((name, user_id)) = &cached_identity {
                        // Client already removed from global state (e.g. cleanup
                        // race), but we cached its identity at login — still name it.
                        let notice = format!(
                            "[Server] {} (#{}, {}) timed out (keepalive not received)",
                            name, user_id, addr
                        );
                        let data =
                            packet_util::build_global_chat_packet(b"Server", notice.as_bytes());
                        let _ = crate::handlers::util::broadcast_packet(
                            &global_state,
                            msg::GLOBAL_CHAT,
                            data,
                        )
                        .await;
                    } else {
                        // Never logged in (server-browser probe, stray/out-of-sequence
                        // packet, rejected login): not a real user, so reap silently
                        // without spamming the lobby. Just log it for diagnostics.
                        debug!(
                            { fields::ADDR } = %addr,
                            "Pre-login session reaped (no login); no lobby notice"
                        );
                    }
                    break;
                }
            }
        }

        // Clean up session
        {
            let mut sessions_lock = sessions.write().await;
            sessions_lock.remove(&addr);
        }

        // Clean up from global state (safe even if already removed)
        let _ = global_state.remove_client(&addr).await;

        debug!("Session terminated");
    }
    .instrument(span)
    .await
}
