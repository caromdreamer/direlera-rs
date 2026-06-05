pub mod chat;
pub mod game;
pub mod sync;
pub mod user;
pub mod util;

use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::kaillera::message_types as msg;
use crate::*;

// You can use #[instrument(skip(state))] to prevent selected arguments (like 'state') from being included in the default tracing output.
// This helps avoid logging large or sensitive data, or types that do not implement Debug.
// For tracing, all arguments except those listed in skip() will be captured.
// Always use English comments.
// Only skip 'state' and 'message' in tracing, do not skip 'src' so that 'src' appears in tracing logs.
#[instrument(level = "debug", skip(state, message))]
pub async fn handle_message(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    metrics::counter!("packets_received_total", "type" => crate::kaillera::message_types::message_type_name(message.message_type)).increment(1);
    match message.message_type {
        msg::USER_QUIT => user::handle_user_quit(message, src, state).await?,
        msg::USER_LOGIN => user::handle_user_login(message, src, state).await?,
        msg::CLIENT_TO_SERVER_ACK => handle_client_to_server_ack(src, state).await?,
        msg::GLOBAL_CHAT => chat::handle_global_chat(message, src, state).await?,
        msg::GAME_CHAT => chat::handle_game_chat(message, src, state).await?,
        msg::CLIENT_KEEP_ALIVE => handle_client_keep_alive(message, src, state).await?,
        msg::CREATE_GAME => game::handle_create_game(message, src, state).await?,
        msg::QUIT_GAME => game::handle_quit_game(message.data, src, state).await?,
        msg::JOIN_GAME => game::handle_join_game(message, src, state).await?,
        msg::KICK_USER => game::handle_kick_user(message, src, state).await?,
        msg::START_GAME => game::handle_start_game(message, src, state).await?,
        msg::GAME_DATA => {
            debug!(
                message_type = msg::message_type_name(message.message_type),
                "Game sync request received"
            );
            sync::handle_game_data(message, src, state).await?;
        }
        msg::GAME_CACHE => sync::handle_game_cache(message, src, state).await?,
        msg::DROP_GAME => game::handle_drop_game(message, src, state).await?,
        msg::READY_TO_PLAY => sync::handle_ready_to_play_signal(message, src, state).await?,

        _ => {
            warn!(
                message_type = msg::message_type_name(message.message_type),
                "Unknown message type received"
            );
        }
    }
    Ok(())
}

pub async fn handle_client_to_server_ack(
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    // Client to Server ACK [0x06]
    // Calculate ping (RTT: time from sending SERVER_TO_CLIENT_ACK to receiving CLIENT_TO_SERVER_ACK)
    // Average of last 5 measurements, excluding the first measurement
    // and update ack count
    let ack_count = state
        .update_client(src, |client_info| {
            if let Some(last_ping_time) = client_info.last_ping_time {
                // Calculate round-trip time (RTT) from when we sent SERVER_TO_CLIENT_ACK
                let current_rtt = last_ping_time.elapsed().as_millis() as u32;

                client_info.ack_count += 1;

                // Skip first measurement (ack_count == 1)
                if client_info.ack_count > 1 {
                    // Add current RTT to samples
                    client_info.ping_samples.push(current_rtt);

                    // Keep only last 5 measurements
                    if client_info.ping_samples.len() > 5 {
                        client_info.ping_samples.remove(0);
                    }

                    // Calculate average of collected samples
                    if !client_info.ping_samples.is_empty() {
                        let sum: u32 = client_info.ping_samples.iter().sum();
                        client_info.ping = sum / client_info.ping_samples.len() as u32;
                    }
                }
            }
            // Note: last_ping_time will be updated when we send SERVER_TO_CLIENT_ACK below
            Ok(client_info.ack_count)
        })
        .await?;

    if ack_count >= 3 {
        let data = util::make_server_status(src, &state).await?;
        util::send_packet(&state, src, msg::SERVER_STATUS, data).await?;

        let data = util::make_user_joined(src, &state).await?;
        util::broadcast_packet(&state, msg::USER_JOINED, data).await?;

        // Welcome message is sent one packet per line: the Kaillera client
        // treats each SERVER_INFORMATION as a single chat line and truncates at
        // the first embedded newline.
        let info_lines = util::make_server_information(&state, src).await?;
        for data in info_lines {
            util::send_packet(&state, src, msg::SERVER_INFORMATION, data).await?;
        }
    } else {
        // Server notification creation
        let data = packet_util::build_server_to_client_ack_packet();
        util::send_packet(&state, src, msg::SERVER_TO_CLIENT_ACK, data).await?;
    }

    Ok(())
}

pub async fn handle_client_keep_alive(
    _message: kaillera::protocol::ParsedMessage,
    _src: &std::net::SocketAddr,
    _state: Arc<AppState>,
) -> color_eyre::Result<()> {
    // Matching EmuLinker-K (KeepAliveAction): keep-alive only refreshes the
    // client's activity timestamp, which already happens in the session loop
    // (update_client_activity). The server sends NO response.
    //
    // Previously this re-sent SERVER_STATUS (the full user+game list) on every
    // keep-alive. Clients append received list entries, so the periodic re-send
    // made the same users pile up — showing duplicate users in the list.
    Ok(())
}
