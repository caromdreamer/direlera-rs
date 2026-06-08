use bytes::{Buf, BytesMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

const SESSION_ALIVE_THRESHOLD_SECS: u64 = 30;

use super::util;
use crate::kaillera::message_types as msg;
use crate::*;

#[tracing::instrument(skip_all)]
pub async fn handle_user_login(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
    session_span: tracing::Span,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Username (read as bytes to preserve encoding)
    let mut username = util::read_string_bytes(&mut buf);
    // NB: Emulator Name (read as bytes to preserve encoding)
    let emulator_name = util::read_string_bytes(&mut buf);
    // 1B: Connection Type
    let conn_type = if !buf.is_empty() { buf.get_u8() } else { 0 };

    // Validate username length (31 bytes max - not characters, to preserve encoding)
    if username.len() > 31 {
        use tracing::warn;
        warn!(
            "Username too long ({} bytes), truncating to 31",
            username.len()
        );
        // Truncate to 31 bytes
        username.truncate(31);
    }

    // Duplicate username handling: evict stale sessions, reject active ones.
    if let Some((old_addr, old_client)) = state.find_client_by_username(&username).await {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last_active = old_client.last_activity_secs.load(Ordering::Relaxed);
        let is_alive = now_secs.saturating_sub(last_active) < SESSION_ALIVE_THRESHOLD_SECS;

        if is_alive {
            warn!(
                "Login rejected: username already in use by active session at {}",
                old_addr
            );
            // Tell the client explicitly with CONNECTION_REJECTED (0x16) instead of
            // dropping silently (which the client can't distinguish from a network
            // failure — it just times out). This is the first and only server reply
            // to the not-yet-registered login attempt, so build it with a fresh
            // generator (sequence starts at 0, like the normal ACK would).
            let mut gen = kaillera::protocol::UDPPacketGenerator::new();
            let body = packet_util::build_connection_rejected_packet(
                b"server",
                0,
                b"Username is already in use.",
            );
            let datagram = gen.make_send_packet(msg::CONNECTION_REJECTED, body);
            let _ = state
                .tx
                .send(crate::Message {
                    data: datagram,
                    addr: *src,
                })
                .await;
            return Ok(());
        }

        // Tear down any game the stale session was in BEFORE removing its client.
        // remove_client_by_username only drops the client maps — it doesn't touch
        // games — so without this an owned, still-playing game would be orphaned
        // into a ghost room (listed as playing with no live members). Running the
        // normal quit flow here closes the game (or migrates ownership if other
        // players remain). Must run while the old client is still in global state,
        // since handle_quit_game looks it up by address.
        if old_client.game_id.is_some() {
            let _ =
                crate::handlers::game::handle_quit_game(Vec::new(), &old_addr, state.clone()).await;
        }

        // Stale session — evict and allow reconnect
        if let Some((evicted_addr, evicted)) = state.remove_client_by_username(&username).await {
            info!(
                "Evicting stale session for reconnecting user (old session at {})",
                old_addr
            );
            let quit_data = packet_util::build_user_quit_packet(
                &evicted.username,
                evicted.user_id,
                b"reconnected",
            );
            util::broadcast_packet(&state, msg::USER_QUIT, quit_data).await?;
            // Also drop the old UDP session so it doesn't time out later.
            state.close_session(&evicted_addr).await;
        }
    }

    // Lock-free ID generation
    let user_id = state.next_user_id();
    let session_id = Uuid::new_v4();

    // Stable identity is now known — stamp it on the session span once. Every
    // subsequent handler log for this session inherits these fields.
    session_span.record("user_name", util::bytes_for_log(&username).as_str());
    session_span.record("user_id", user_id);
    session_span.record("connection_type", conn_type);
    session_span.record("session_id", session_id.to_string().as_str());

    info!(
        "User logged in (emulator {})",
        util::bytes_for_log(&emulator_name)
    );

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let client = ClientInfo {
        session_id,
        username,
        emulator_name,
        conn_type,
        user_id,
        ping: 0,
        player_status: PLAYER_STATUS_IDLE,
        game_id: None,
        last_ping_time: Some(Instant::now()),
        ack_count: 0,
        ping_total: std::time::Duration::ZERO,
        last_activity_secs: Arc::new(std::sync::atomic::AtomicU64::new(now_secs)),
        packet_generator: kaillera::protocol::UDPPacketGenerator::new(),
        session_span,
    };

    // Encapsulated method
    state.add_client(*src, client).await;

    // Prepare response data
    let data = packet_util::build_server_to_client_ack_packet();

    // Send response
    util::send_packet(&state, src, msg::SERVER_TO_CLIENT_ACK, data).await?;
    util::record_processing_time("user_login", start.elapsed());
    Ok(())
}

/*
'            Server Notification:
'            NB : Username
'            2B : UserID
'            NB : Message
 */
#[tracing::instrument(skip_all)]
pub async fn handle_user_quit(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    use tracing::debug;
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Empty String
    let _empty = util::read_string_bytes(&mut buf);
    // 2B: 0xFF
    let _code = if buf.len() >= 2 { buf.get_u16_le() } else { 0 };
    // NB: Message (read as bytes to preserve encoding)
    let user_message = util::read_string_bytes(&mut buf);

    // Handle quit game first
    super::game::handle_quit_game(vec![0x00, 0xFF, 0xFF], src, state.clone()).await?;

    // Remove client from list
    if let Some(client_info) = state.remove_client(src).await {
        info!("User quit: {}", String::from_utf8_lossy(&user_message));
        let data = packet_util::build_user_quit_packet(
            &client_info.username,
            client_info.user_id,
            &user_message,
        );
        util::broadcast_packet(&state, msg::USER_QUIT, data).await?;
    } else {
        debug!(
            "Unknown client quit: {}",
            String::from_utf8_lossy(&user_message)
        );
    }
    // Tear down the UDP session now so the orphaned session task doesn't linger
    // until SESSION_TIMEOUT and emit a spurious "timed out" notice.
    state.close_session(src).await;
    util::record_processing_time("user_quit", start.elapsed());
    Ok(())
}
