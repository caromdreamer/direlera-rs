use std::sync::Arc;

use crate::*;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io;
use tokio::net::UdpSocket;
use tracing::{debug, error};

#[allow(dead_code)]
pub async fn handle_control_socket(
    control_socket: Arc<UdpSocket>,
    main_port: u16,
) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let (len, src) = control_socket.recv_from(&mut buf).await?;
        let data = &buf[..len];

        // Handle the HELLO0.83 message
        if data == b"HELLO0.83\x00" {
            debug!(
                { fields::ADDR } = %src,
                { fields::PORT } = main_port,
                "HELLO request received on control socket"
            );
            let response = format!("HELLOD00D{}\0", main_port).into_bytes();
            control_socket.send_to(&response, src).await?;
        }
        // Handle the PING message
        else if data == b"PING\x00" {
            let response = b"PONG\x00".to_vec();
            control_socket.send_to(&response, src).await?;
        } else {
            let ascii_string: String = data
                .iter()
                .map(|&b| if b.is_ascii() { b as char } else { '.' }) // Replace invalid with '.'
                .collect();
            error!(
                { fields::ADDR } = %src,
                { fields::PACKET_SIZE } = data.len(),
                message_preview = &ascii_string[..ascii_string.len().min(50)],
                "Unknown message on control socket"
            );
        }
    }
}

pub fn parse_packet(data: &[u8]) -> Result<Vec<kaillera::protocol::ParsedMessage>, String> {
    let mut buf = BytesMut::from(data);
    if buf.is_empty() {
        return Err("Packet is empty.".to_string());
    }
    let num_messages = buf.get_u8();

    // Log detailed info for debugging
    if num_messages == 0 {
        return Err(format!(
            "Invalid packet: num_messages is 0, total_size={}, remaining={}",
            data.len(),
            buf.len()
        ));
    }

    let mut messages = Vec::new();

    for i in 0..num_messages {
        if buf.len() < 5 {
            return Err(format!(
                "Incomplete message header at message {}/{}. Buffer size: {}, total_packet_size: {}, first_bytes: {:02x?}",
                i + 1,
                num_messages,
                buf.len(),
                data.len(),
                &data[..data.len().min(10)]
            ));
        }

        let message_number = buf.get_u16_le();
        let message_length = buf.get_u16_le();
        let message_type = buf.get_u8();

        if buf.len() < (message_length - 1) as usize {
            return Err("Incomplete message data.".to_string());
        }

        let message_data = buf.split_to((message_length - 1) as usize);

        messages.push(kaillera::protocol::ParsedMessage {
            message_number,
            message_length,
            message_type,
            data: message_data.to_vec(),
        });
    }

    Ok(messages)
}

// Packet building utilities

/// Appends bytes with null terminator to the buffer (preserves original encoding)
pub fn put_bytes_with_null(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put(bytes);
    buf.put_u8(0);
}

/// Appends a string with null terminator to the buffer (converts to UTF-8 bytes)
#[allow(dead_code)]
pub fn put_string_with_null(buf: &mut BytesMut, s: &str) {
    put_bytes_with_null(buf, s.as_bytes());
}

/// Appends an empty string (just null terminator) to the buffer
pub fn put_empty_string(buf: &mut BytesMut) {
    buf.put_u8(0);
}

/// Appends multiple strings with null terminators to the buffer
#[allow(dead_code)]
pub fn put_strings_with_null(buf: &mut BytesMut, strings: &[&str]) {
    for s in strings {
        put_string_with_null(buf, s);
    }
}

// Packet builder functions - return Vec<u8> for packet data

/// Builds a START_GAME packet
/// Format: Empty String [00], Frame Delay (2B), Player Number (1B), Total Players (1B)
pub fn build_start_game_packet(frame_delay: u16, player_number: u8, total_players: u8) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_empty_string(&mut data);
    data.put_u16_le(frame_delay);
    data.put_u8(player_number);
    data.put_u8(total_players);
    data.to_vec()
}

/// Builds a GAME_CHAT packet
/// Format: Username (NB), Message (NB)
pub fn build_game_chat_packet(username: &[u8], message: &[u8]) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    put_bytes_with_null(&mut data, message);
    data.to_vec()
}

/// Builds a GLOBAL_CHAT packet
/// Format: Username (NB), Message (NB)
pub fn build_global_chat_packet(username: &[u8], message: &[u8]) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    put_bytes_with_null(&mut data, message);
    data.to_vec()
}

/// Builds a USER_QUIT packet
/// Format: Username (NB), UserID (2B), Message (NB)
pub fn build_user_quit_packet(username: &[u8], user_id: u16, message: &[u8]) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    data.put_u16_le(user_id);
    put_bytes_with_null(&mut data, message);
    data.to_vec()
}

/// Builds a QUIT_GAME packet
/// Format: Username (NB), UserID (2B)
pub fn build_quit_game_packet(username: &[u8], user_id: u16) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    data.put_u16_le(user_id);
    data.to_vec()
}

/// Builds a CLOSE_GAME packet
/// Format: Empty String [00], GameID (4B)
/// 0x16 Connection Rejected body: username (sender) + UserID (2B LE) + message.
pub fn build_connection_rejected_packet(username: &[u8], user_id: u16, message: &[u8]) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    data.put_u16_le(user_id);
    put_bytes_with_null(&mut data, message);
    data.to_vec()
}

pub fn build_close_game_packet(game_id: u32) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_empty_string(&mut data);
    data.put_u32_le(game_id);
    data.to_vec()
}

/// Builds a READY_TO_PLAY packet
/// Format: Empty String [00]
pub fn build_ready_to_play_packet() -> Vec<u8> {
    let mut data = BytesMut::new();
    put_empty_string(&mut data);
    data.to_vec()
}

/// Builds a SERVER_TO_CLIENT_ACK packet
/// Format: Empty String [00], 0 (4B), 1 (4B), 2 (4B), 3 (4B)
pub fn build_server_to_client_ack_packet() -> Vec<u8> {
    let mut data = BytesMut::new();
    put_empty_string(&mut data);
    data.put_u32_le(0);
    data.put_u32_le(1);
    data.put_u32_le(2);
    data.put_u32_le(3);
    data.to_vec()
}

/// Builds a DROP_GAME packet
/// Format: Username (NB), Player Number (1B)
pub fn build_drop_game_packet(username: &[u8], player_number: u8) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_bytes_with_null(&mut data, username);
    data.put_u8(player_number);
    data.to_vec()
}

/// Builds a GAME_DATA packet
/// Format: Empty String [00], Data Length (2B), Game Data (NB)
pub fn build_game_data_packet(game_data: &[u8]) -> Vec<u8> {
    let mut data = BytesMut::new();
    put_empty_string(&mut data);
    data.put_u16_le(game_data.len() as u16);
    data.put(game_data);
    data.to_vec()
}

/// Builds a GAME_CACHE packet
/// Format: Empty String [00], Position (1B)
pub fn build_game_cache_packet(position: u8) -> Vec<u8> {
    vec![0x00, position]
}
