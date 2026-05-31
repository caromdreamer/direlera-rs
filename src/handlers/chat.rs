use crate::*;
use bytes::BytesMut;
use color_eyre::eyre::{eyre, WrapErr};
use std::sync::Arc;
use tracing::info;

use super::util;
use crate::kaillera::message_types as msg;

#[tracing::instrument(skip(message, state), fields(
    addr = %src,
    username = tracing::field::Empty,
    session_id = tracing::field::Empty,
))]
pub async fn handle_global_chat(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Empty String
    let _empty = util::read_string_bytes(&mut buf);
    // NB: Message (read as bytes to preserve encoding)
    let chat_message = util::read_string_bytes(&mut buf);

    let username = if let Some(client_info) = state.get_client(src).await {
        util::record_session_fields(&client_info);
        client_info.username.clone()
    } else {
        b"Unknown".to_vec()
    };

    info!(
        "Global chat message: {}",
        util::bytes_to_string(&chat_message)
    );

    let data = packet_util::build_global_chat_packet(&username, &chat_message);
    util::broadcast_packet(&state, msg::GLOBAL_CHAT, data)
        .await
        .wrap_err("Failed to broadcast global chat message")?;

    Ok(())
}

#[tracing::instrument(skip(message, state), fields(
    addr = %src,
    username = tracing::field::Empty,
    session_id = tracing::field::Empty,
    game_id = tracing::field::Empty,
))]
pub async fn handle_game_chat(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let mut buf = BytesMut::from(&message.data[..]);

    // NB: Empty String
    let _empty = util::read_string_bytes(&mut buf);
    // NB: Message (read as bytes to preserve encoding)
    let chat_message = util::read_string_bytes(&mut buf);

    let client_info = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    util::record_session_fields(&client_info);

    let game_id = client_info
        .game_id
        .ok_or_else(|| eyre!("Client attempted game chat but not in a game"))?;

    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    if !game_info.players.iter().any(|p| p.addr == *src) {
        use tracing::warn;
        warn!(
            { fields::USER_NAME } = client_info.username_str().as_str(),
            { fields::GAME_ID } = game_id,
            "User attempted game chat but not in game players list"
        );
        return Ok(());
    }

    if chat_message.contains(&0x11) {
        info!("skipping game chat message containing 0x11");
        return Ok(());
    }

    info!(
        "Game chat message: {}",
        util::bytes_to_string(&chat_message)
    );

    let data = packet_util::build_game_chat_packet(&client_info.username, &chat_message);
    util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, data)
        .await
        .wrap_err_with(|| format!("Failed to broadcast game chat to game {}", game_id))?;

    Ok(())
}
