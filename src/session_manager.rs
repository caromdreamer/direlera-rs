// Session-based UDP handling - TCP-like session management for UDP
// Each client gets its own session handler, similar to TCP connections

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tracing::{info, warn, Instrument};

use crate::{fields, packet_util, AppState};

/// Configuration for session timeout behavior
const SESSION_TIMEOUT: Duration = Duration::from_secs(120);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(3);

/// Stall recovery: if a playing client hasn't sent game input for this long, it
/// likely missed a server->client packet (lockstep freeze). Resend its last
/// packet so it can catch up. Checked every STALL_RESEND_INTERVAL.
const STALL_THRESHOLD: Duration = Duration::from_millis(100);
const STALL_RESEND_INTERVAL: Duration = Duration::from_millis(50);

/// Represents a single UDP "session" - simulating TCP connection
struct UdpSession {
    last_seen: Instant,
    tx: mpsc::Sender<Vec<u8>>,
}

/// Manages all active UDP sessions
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<SocketAddr, UdpSession>>>,
    packet_tx: mpsc::Sender<(SocketAddr, Vec<u8>)>,
}

impl SessionManager {
    pub fn new() -> (Self, mpsc::Receiver<(SocketAddr, Vec<u8>)>) {
        let (packet_tx, packet_rx) = mpsc::channel(1000);

        let manager = Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            packet_tx,
        };

        (manager, packet_rx)
    }

    /// Get the sender for the main UDP dispatcher to send packets
    pub fn packet_sender(&self) -> mpsc::Sender<(SocketAddr, Vec<u8>)> {
        self.packet_tx.clone()
    }

    /// Start the session manager - spawns session handlers as needed
    pub async fn run(
        self: Arc<Self>,
        mut packet_rx: mpsc::Receiver<(SocketAddr, Vec<u8>)>,
        global_state: Arc<AppState>,
    ) {
        info!("Session manager started");

        while let Some((addr, data)) = packet_rx.recv().await {
            let sessions = self.sessions.read().await;

            if let Some(session) = sessions.get(&addr) {
                // Existing session - forward packet
                if let Err(e) = session.tx.send(data).await {
                    warn!(
                        { fields::ADDR } = %addr,
                        { fields::ERROR } = %e,
                        "Failed to forward packet to session"
                    );
                }
            } else {
                // New session - spawn handler
                drop(sessions);
                self.spawn_session_handler(addr, data, global_state.clone())
                    .await;
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

        info!({ fields::ADDR } = %addr, "New session spawned");

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

            // Collect addresses of stalled players in playing games.
            let stalled: Vec<SocketAddr> = {
                let game_ids: Vec<u32> =
                    { global_state.games.read().await.keys().copied().collect() };
                let mut out = Vec::new();
                for gid in game_ids {
                    if let Some(game) = global_state.get_game(gid).await {
                        if game.game_status != crate::state::GAME_STATUS_PLAYING {
                            continue;
                        }
                        for p in &game.players {
                            if let Some(last) = p.last_game_data_recv {
                                if now.duration_since(last) >= STALL_THRESHOLD {
                                    out.push(p.addr);
                                }
                            }
                        }
                    }
                }
                out
            };

            // Resend each stalled client's last packet verbatim (same msg numbers).
            for addr in stalled {
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
                }
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
    let span = tracing::info_span!("session", addr = %addr);
    async move {
        info!("Session handler started");

        // Per-session packet counter — no global lock needed
        let mut packet_counter: u16 = 0;

        // Session loop - similar to TCP recv loop
        loop {
            match timeout(SESSION_TIMEOUT, rx.recv()).await {
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
                    )
                    .await;
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
                    warn!(timeout_duration = ?SESSION_TIMEOUT, "Session timeout");
                    use crate::kaillera::message_types as msg;
                    if let Some(client_info) = global_state.get_client(&addr).await {
                        let username = crate::handlers::util::bytes_for_log(&client_info.username);
                        // 글로벌 채팅으로 타임아웃 알림 (디버깅용)
                        let notice =
                            format!("[Server] {} timed out (keepalive not received)", username);
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
                    } else {
                        // 로그인 안 한 채 타임아웃 (주소만 알림)
                        let notice = format!("[Server] {} timed out (no login)", addr);
                        let data =
                            packet_util::build_global_chat_packet(b"Server", notice.as_bytes());
                        let _ = crate::handlers::util::broadcast_packet(
                            &global_state,
                            msg::GLOBAL_CHAT,
                            data,
                        )
                        .await;
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

        info!("Session terminated");
    }
    .instrument(span)
    .await
}
