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

#[tracing::instrument(skip(message, state), fields(
    addr = %src,
    username = tracing::field::Empty,
    session_id = tracing::field::Empty,
))]
pub async fn handle_user_login(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Username (read as bytes to preserve encoding)
    let mut username = util::read_string_bytes(&mut buf);
    // NB: Emulator Name (read as bytes to preserve encoding)
    let emulator_name = util::read_string_bytes(&mut buf);
    // 1B: Connection Type
    let conn_type = if !buf.is_empty() { buf.get_u8() } else { 0 };

    tracing::Span::current().record("username", util::bytes_for_log(&username).as_str());

    // Validate username length (31 bytes max - not characters, to preserve encoding)
    if username.len() > 31 {
        use tracing::warn;
        warn!(
            username_len = username.len(),
            "Username too long, truncating to 31 bytes"
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
                old_addr = %old_addr,
                { fields::USER_NAME } = util::bytes_for_log(&username).as_str(),
                "Login rejected: username already in use by active session"
            );
            return Ok(());
        }

        // Stale session — evict and allow reconnect
        if let Some((_, evicted)) = state.remove_client_by_username(&username).await {
            info!(
                old_addr = %old_addr,
                { fields::USER_NAME } = util::bytes_for_log(&evicted.username).as_str(),
                "Evicting stale session for reconnecting user"
            );
            let quit_data = packet_util::build_user_quit_packet(
                &evicted.username,
                evicted.user_id,
                b"reconnected",
            );
            util::broadcast_packet(&state, msg::USER_QUIT, quit_data).await?;
        }
    }

    // Lock-free ID generation
    let user_id = state.next_user_id();

    info!(
        { fields::USER_NAME } = util::bytes_for_log(&username).as_str(),
        { fields::USER_ID } = user_id,
        emulator = util::bytes_for_log(&emulator_name).as_str(),
        { fields::CONNECTION_TYPE } = conn_type,
        "User logged in"
    );

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let client = ClientInfo {
        session_id: Uuid::new_v4(),
        username,
        emulator_name,
        conn_type,
        user_id,
        ping: 0,
        player_status: PLAYER_STATUS_IDLE,
        game_id: None,
        last_ping_time: Some(Instant::now()),
        ack_count: 0,
        ping_samples: Vec::new(),
        last_activity_secs: Arc::new(std::sync::atomic::AtomicU64::new(now_secs)),
        packet_generator: kaillera::protocol::UDPPacketGenerator::new(),
    };

    // Encapsulated method
    state.add_client(*src, client).await;

    // Prepare response data
    let data = packet_util::build_server_to_client_ack_packet();

    // Send response
    util::send_packet(&state, src, msg::SERVER_TO_CLIENT_ACK, data).await?;

    Ok(())
}

/*
'            Server Notification:
'            NB : Username
'            2B : UserID
'            NB : Message
 */
#[tracing::instrument(skip(message, state), fields(
    addr = %src,
    username = tracing::field::Empty,
    session_id = tracing::field::Empty,
))]
pub async fn handle_user_quit(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    use tracing::debug;
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Empty String
    let _empty = util::read_string_bytes(&mut buf);
    // 2B: 0xFF
    let _code = if buf.len() >= 2 { buf.get_u16_le() } else { 0 };
    // NB: Message (read as bytes to preserve encoding)
    let user_message = util::read_string_bytes(&mut buf);

    if let Some(client) = state.get_client(src).await {
        util::record_session_fields(&client);
    }

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
            quit_message = String::from_utf8_lossy(&user_message).as_ref(),
            "Unknown client quit"
        );
    }
    Ok(())
}
