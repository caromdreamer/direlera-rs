use bytes::{Buf, BufMut, BytesMut};
use color_eyre::eyre::eyre;
use encoding_rs::{Encoding, EUC_KR, GBK, SHIFT_JIS, UTF_8};
use tracing::{debug, info};

use crate::{packet_util, state, Message};

use state::{AppState, ClientInfo, GameInfo};

/// Record packet_processing_seconds histogram for a given message type.
pub fn record_processing_time(msg_type: &'static str, elapsed: std::time::Duration) {
    metrics::histogram!("packet_processing_seconds", "type" => msg_type)
        .record(elapsed.as_secs_f64());
}

/// Record session context (username, session_id, game_id) onto the current span.
/// Call this at the top of any handler after fetching ClientInfo.
pub fn record_session_fields(client: &ClientInfo) {
    let span = tracing::Span::current();
    span.record("username", client.username_str().as_str());
    span.record("session_id", client.session_id.to_string().as_str());
    if let Some(game_id) = client.game_id {
        span.record("game_id", game_id);
    }
}

pub fn build_join_game_response(user: &ClientInfo) -> Vec<u8> {
    let mut data = BytesMut::new();
    packet_util::put_empty_string(&mut data);
    data.put_u32_le(0); // Pointer to Game on Server Side
    packet_util::put_bytes_with_null(&mut data, &user.username);
    data.put_u32_le(user.ping);
    data.put_u16_le(user.user_id);
    data.put_u8(user.conn_type);
    data.to_vec()
}

pub fn build_new_game_notification(
    username: &[u8],
    game_name: &[u8],
    emulator_name: &[u8],
    game_id: u32,
) -> Vec<u8> {
    let mut data = BytesMut::new();
    packet_util::put_bytes_with_null(&mut data, username);
    packet_util::put_bytes_with_null(&mut data, game_name);
    packet_util::put_bytes_with_null(&mut data, emulator_name);
    data.put_u32_le(game_id);
    data.to_vec()
}

// '     0x0D = Player Information
// '            Server Notification:
// '            NB : Empty String [00]
// '            4B : Number of Users in Room [not including you]
// '            NB : Username
// '            4B : Ping
// '            2B : UserID
// '            1B : Connection Type (6=Bad, 5=Low, 4=Average, 3=Good, 2=Excellent, & 1=LAN)
pub async fn make_player_information(
    src: &std::net::SocketAddr,
    state: &AppState,
    game_info: &GameInfo,
) -> color_eyre::Result<Vec<u8>> {
    // Prepare response data
    let mut data = BytesMut::new();
    packet_util::put_empty_string(&mut data);
    // Number of users in room (excluding self)
    // Safe: players.len() is always >= 1 when this function is called (at least the caller is in the game)
    let player_count = game_info.players.len().saturating_sub(1);
    data.put_u32_le(player_count as u32);

    debug!(
        PLAYER_COUNT = game_info.players.len(),
        "Building player information"
    );

    for player in &game_info.players {
        if player.addr != *src {
            // Get current ping from ClientInfo (it can change during game)
            let ping = state
                .get_client(&player.addr)
                .await
                .map(|c| c.ping)
                .unwrap_or(0);
            packet_util::put_bytes_with_null(&mut data, &player.username);
            data.put_u32_le(ping);
            data.put_u16_le(player.user_id);
            data.put_u8(player.conn_type);
        }
    }
    Ok(data.to_vec())
}

// '     0x0E = Update Game Status
// '            Server Notification:
// '            NB : Empty String [00]
// '            4B : GameID
// '            1B : Game Status (0=Waiting, 1=Playing, 2=Netsync)
// '            1B : Number of Players in Room
// '            1B : Maximum Players
pub fn make_update_game_status(game_info: &GameInfo) -> color_eyre::Result<Vec<u8>> {
    let mut data = BytesMut::new();
    packet_util::put_empty_string(&mut data);
    data.put_u32_le(game_info.game_id);
    data.put_u8(game_info.game_status);
    data.put_u8(game_info.num_players);
    data.put_u8(game_info.max_players);
    Ok(data.to_vec())
}

pub async fn make_user_joined(
    src: &std::net::SocketAddr,
    state: &AppState,
) -> color_eyre::Result<Vec<u8>> {
    let client_info = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;

    let mut data = BytesMut::new();
    packet_util::put_bytes_with_null(&mut data, &client_info.username);
    data.put_u16_le(client_info.user_id);
    data.put_u32_le(client_info.ping);
    data.put_u8(client_info.conn_type);
    Ok(data.to_vec())
}

// Helper functions
pub async fn send_packet(
    state: &AppState,
    addr: &std::net::SocketAddr,
    packet_type: u8,
    data: Vec<u8>,
) -> color_eyre::Result<()> {
    use crate::kaillera::message_types as msg;
    use std::time::Instant;

    let response_packet = state
        .update_client(addr, |client| {
            // Record timestamp when sending SERVER_TO_CLIENT_ACK for ping measurement
            if packet_type == msg::SERVER_TO_CLIENT_ACK {
                client.last_ping_time = Some(Instant::now());
            }
            Ok(client.packet_generator.make_send_packet(packet_type, data))
        })
        .await?;

    state
        .tx
        .send(Message {
            data: response_packet,
            addr: *addr,
        })
        .await?;
    Ok(())
}

pub async fn broadcast_packet(
    state: &AppState,
    packet_type: u8,
    data: Vec<u8>,
) -> color_eyre::Result<()> {
    // Get all client addresses first
    let client_addrs = state.get_all_client_addrs().await;

    // Generate packets for all clients
    let packets: Vec<_> = {
        let addr_map = state.clients_by_addr.read().await;
        let mut id_map = state.clients_by_id.write().await;

        client_addrs
            .iter()
            .filter_map(|addr| {
                let session_id = addr_map.get(addr)?;
                let client = id_map.get_mut(session_id)?;
                let packet = client
                    .packet_generator
                    .make_send_packet(packet_type, data.clone());
                Some((*addr, packet))
            })
            .collect()
    };

    // Send packets without holding the lock
    for (addr, packet) in packets {
        state.tx.send(Message { data: packet, addr }).await?;
    }
    Ok(())
}

/// Broadcasts a packet to all players in a specific game
pub async fn broadcast_packet_to_game(
    state: &AppState,
    game_id: u32,
    packet_type: u8,
    data: Vec<u8>,
) -> color_eyre::Result<()> {
    // Get game info to get player list
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;

    // Generate packets for all players in the game
    let packets: Vec<_> = {
        let addr_map = state.clients_by_addr.read().await;
        let mut id_map = state.clients_by_id.write().await;

        game_info
            .players
            .iter()
            .filter_map(|player| {
                let session_id = addr_map.get(&player.addr)?;
                let client = id_map.get_mut(session_id)?;
                // Verify client is still in the same game
                if client.game_id != Some(game_id) {
                    return None;
                }
                let packet = client
                    .packet_generator
                    .make_send_packet(packet_type, data.clone());
                Some((player.addr, packet))
            })
            .collect()
    };

    // Send packets without holding the lock
    for (addr, packet) in packets {
        state.tx.send(Message { data: packet, addr }).await?;
    }
    Ok(())
}

/// Read a null-terminated string as bytes (preserves original encoding)
pub fn read_string_bytes(buf: &mut BytesMut) -> Vec<u8> {
    let mut s = Vec::new();
    while let Some(&b) = buf.first() {
        buf.advance(1);
        if b == 0 {
            break;
        }
        s.push(b);
    }
    s
}

/// Read a null-terminated string as String (for backward compatibility, uses lossy conversion)
#[allow(dead_code)]
pub fn read_string(buf: &mut BytesMut) -> String {
    let bytes = read_string_bytes(buf);
    String::from_utf8_lossy(&bytes).to_string()
}

/// Language detection result
#[derive(Debug, Clone, Copy)]
struct LanguageDetection {
    #[allow(dead_code)]
    korean_count: usize,
    #[allow(dead_code)]
    japanese_count: usize,
    #[allow(dead_code)]
    chinese_count: usize,
    has_korean: bool,
    has_japanese: bool,
    has_chinese: bool,
}

/// Detect language from text by analyzing Unicode character ranges
fn detect_language(text: &str) -> LanguageDetection {
    let mut korean_count = 0;
    let mut japanese_count = 0;
    let mut chinese_count = 0;

    for ch in text.chars() {
        let code = ch as u32;

        // Korean: Hangul Syllables (완성형 한글) and Hangul Jamo (자모: 초성/중성/종성)
        // Hangul Syllables: 0xAC00-0xD7AF
        // Hangul Jamo: 0x1100-0x11FF (초성/중성/종성)
        // Hangul Compatibility Jamo: 0x3130-0x318F (호환용 자모, 초성만 입력할 때 사용)
        if (0xAC00..=0xD7AF).contains(&code)
            || (0x1100..=0x11FF).contains(&code)
            || (0x3130..=0x318F).contains(&code)
        {
            korean_count += 1;
        }
        // Japanese: Hiragana (0x3040-0x309F), Katakana (0x30A0-0x30FF)
        else if (0x3040..=0x309F).contains(&code) || (0x30A0..=0x30FF).contains(&code) {
            japanese_count += 1;
        }
        // Chinese: CJK Unified Ideographs (0x4E00-0x9FFF)
        // Note: This range is shared with Japanese/Chinese, but we prioritize Hiragana/Katakana for Japanese
        else if (0x4E00..=0x9FFF).contains(&code) {
            chinese_count += 1;
        }
    }

    LanguageDetection {
        korean_count,
        japanese_count,
        chinese_count,
        has_korean: korean_count > 0,
        has_japanese: japanese_count > 0,
        has_chinese: chinese_count > 0,
    }
}

/// Try to decode bytes with multiple encodings and return the best result
/// Returns the decoded string and the encoding name used
fn try_decode_bytes(bytes: &[u8]) -> (String, &'static str) {
    if bytes.is_empty() {
        return (String::new(), "empty");
    }
    debug!(
        bytes_hex = format!("{:02x?}", bytes), // 전체 바이트 출력
        bytes_len = bytes.len(),
        "Trying to decode bytes with multiple encodings"
    );

    // First, try UTF-8 - if it's valid UTF-8, use it immediately
    // UTF-8 is the modern standard and should be preferred when valid
    if let Ok(utf8_str) = std::str::from_utf8(bytes) {
        // Check if it contains non-ASCII characters (indicates it's actually UTF-8, not just ASCII)
        let has_non_ascii = utf8_str.chars().any(|c| !c.is_ascii() && !c.is_control());
        let score = utf8_str
            .chars()
            .filter(|c| !c.is_control() && *c != '\u{FFFD}')
            .count();

        // If it's valid UTF-8 with non-ASCII characters, prefer it
        if has_non_ascii && score > 0 {
            debug!(
                bytes_hex = format!("{:02x?}", &bytes[..bytes.len().min(20)]),
                selected_encoding = "UTF-8",
                score = score,
                "Valid UTF-8 detected, using UTF-8"
            );
            return (utf8_str.to_string(), "UTF-8");
        }
    }

    // If UTF-8 is not valid or only ASCII, try legacy encodings (EUC-KR, Shift-JIS, GBK)
    // These are multi-byte encodings like EUC-KR, not UTF-8
    let encodings: &[(&'static Encoding, &str)] =
        &[(EUC_KR, "EUC-KR"), (SHIFT_JIS, "Shift-JIS"), (GBK, "GBK")];

    let mut best_result: Option<(String, &str, usize)> = None;
    let mut all_results: Vec<(&str, String, usize, bool)> = Vec::new();

    for (encoding, name) in encodings {
        let (decoded, _, had_errors) = encoding.decode(bytes);
        let decoded_str = decoded.into_owned();

        // Score: count printable, non-control characters
        let score = decoded_str
            .chars()
            .filter(|c| !c.is_control() && *c != '\u{FFFD}')
            .count();

        // Prefer results with:
        // 1. No errors
        // 2. Higher score (more printable characters)
        // 3. Contains non-ASCII characters (indicates successful decoding)
        // 4. Bonus: if decoded text contains language-specific characters, prefer that encoding
        let lang_detection = detect_language(&decoded_str);
        let has_language_chars =
            lang_detection.has_korean || lang_detection.has_japanese || lang_detection.has_chinese;

        let is_good = !had_errors
            && score > 0
            && decoded_str
                .chars()
                .any(|c| !c.is_ascii() && !c.is_control());

        // Boost score if language-specific characters are detected
        // Give extra bonus if Korean characters are detected (to prefer EUC-KR over Shift-JIS)
        let final_score = if has_language_chars && is_good {
            let mut bonus = 100; // Base bonus for language-specific characters
            if lang_detection.has_korean && *name == "EUC-KR" {
                bonus += 200; // Extra bonus for Korean text with EUC-KR encoding
            } else if lang_detection.has_japanese && *name == "Shift-JIS" {
                bonus += 200; // Extra bonus for Japanese text with Shift-JIS encoding
            }
            score + bonus
        } else {
            score
        };

        all_results.push((name, decoded_str.clone(), final_score, had_errors));

        if is_good {
            match best_result {
                Some((_, _, best_score)) if final_score > best_score => {
                    best_result = Some((decoded_str, name, final_score));
                }
                None => {
                    best_result = Some((decoded_str, name, final_score));
                }
                _ => {}
            }
        }
    }

    // Log all attempts for debugging
    debug!(
        bytes_hex = format!("{:02x?}", &bytes[..bytes.len().min(20)]),
        "Trying to decode bytes with multiple encodings"
    );
    for (name, decoded, final_score, had_errors) in &all_results {
        // Safely truncate to 30 characters (not bytes) to avoid UTF-8 boundary issues
        let preview = if decoded.chars().count() > 30 {
            format!("{}...", decoded.chars().take(30).collect::<String>())
        } else {
            decoded.clone()
        };
        debug!(
            encoding = name,
            score = final_score,
            had_errors = had_errors,
            preview = preview,
            "Decoding attempt"
        );
    }

    // If we found a good result, return it
    if let Some((result, encoding_name, score)) = best_result {
        // Safely truncate to 50 characters (not bytes) to avoid UTF-8 boundary issues
        let result_preview = if result.chars().count() > 50 {
            format!("{}...", result.chars().take(50).collect::<String>())
        } else {
            result.clone()
        };
        debug!(
            selected_encoding = encoding_name,
            score = score,
            result_preview = result_preview,
            "Selected encoding for decoding"
        );
        return (result, encoding_name);
    }

    // Fallback: try UTF-8 with lossy conversion
    let utf8_result = String::from_utf8_lossy(bytes);
    if utf8_result
        .chars()
        .any(|c| !c.is_control() && c != '\u{FFFD}')
    {
        debug!("Using UTF-8 (lossy) as fallback");
        return (utf8_result.to_string(), "UTF-8 (lossy)");
    }

    // Last resort: show ASCII as-is and hex for others
    debug!("Using hex representation as last resort");
    let mut result = String::new();
    for &byte in bytes {
        if (0x20..=0x7E).contains(&byte) {
            result.push(byte as char);
        } else {
            result.push_str(&format!("\\x{:02x}", byte));
        }
    }
    (result, "hex")
}

/// Convert bytes to a safe string for logging
/// Tries multiple encodings to find the best match
/// This handles CP949, UTF-8, Shift-JIS, GBK, etc.
pub fn bytes_for_log(bytes: &[u8]) -> String {
    let (decoded, _encoding) = try_decode_bytes(bytes);
    decoded
}

/// Convert bytes to a readable string, trying multiple encodings
/// Returns the decoded string (for display/logging purposes)
pub fn bytes_to_string(bytes: &[u8]) -> String {
    let (decoded, _encoding) = try_decode_bytes(bytes);
    decoded
}

/// Detect encoding based on welcome message content
/// Analyzes the message text to determine which encoding should be used
fn detect_encoding_from_message(message: &str) -> &'static Encoding {
    // Check if message has non-ASCII characters
    let has_non_ascii = !message.is_ascii();

    // If no non-ASCII characters, use UTF-8 (most compatible)
    if !has_non_ascii {
        return UTF_8;
    }

    // Use common language detection function
    let lang_detection = detect_language(message);

    // Priority: Japanese > Korean > Chinese Simplified > UTF-8
    if lang_detection.has_korean {
        return EUC_KR;
    }
    if lang_detection.has_japanese {
        return SHIFT_JIS;
    }
    if lang_detection.has_chinese {
        // Default to GBK for Chinese, but could be Big5
        // In practice, GBK is more common
        return GBK;
    }

    // Default to UTF-8 for other cases (European languages, etc.)
    UTF_8
}

pub async fn make_server_information(
    state: &AppState,
    _client_addr: &std::net::SocketAddr,
) -> color_eyre::Result<Vec<u8>> {
    // Prepare response data
    // '            NB : "Server\0"
    // '            NB : Message
    let mut data = BytesMut::new();
    data.put("Server\0".as_bytes());

    // Detect encoding based on welcome message content
    let encoding = detect_encoding_from_message(&state.config.welcome_message);
    info!("Encoding: {}", encoding.name());

    // Append uptime to welcome message
    let uptime = state.start_time.elapsed();
    let uptime_secs = uptime.as_secs();
    let days = uptime_secs / 86400;
    let hours = (uptime_secs % 86400) / 3600;
    let mins = (uptime_secs % 3600) / 60;
    let secs = uptime_secs % 60;
    let uptime_str = if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, mins, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else {
        format!("{}m {}s", mins, secs)
    };
    let full_message = format!("{}\nUptime: {}", state.config.welcome_message, uptime_str);

    // Convert welcome message from UTF-8 (config.toml) to detected encoding
    let (welcome_bytes, _, had_errors) = encoding.encode(&full_message);
    if had_errors {
        debug!(
            encoding = encoding.name(),
            "Some characters could not be encoded, using lossy conversion"
        );
    }

    data.put(welcome_bytes.as_ref());
    data.put_u8(0); // Null terminator

    Ok(data.to_vec())
}
pub async fn make_server_status(
    src: &std::net::SocketAddr,
    state: &AppState,
) -> color_eyre::Result<Vec<u8>> {
    let addr_map = state.clients_by_addr.read().await;
    let id_map = state.clients_by_id.read().await;

    // Collect game Arcs while holding only a brief read lock on the HashMap
    let (num_games, game_arcs) = {
        let games_lock = state.games.read().await;
        (
            games_lock.len() as u32,
            games_lock.values().cloned().collect::<Vec<_>>(),
        )
    };

    // Prepare response data
    let mut data = BytesMut::new();
    packet_util::put_empty_string(&mut data);

    // Number of users (excluding self)
    let num_users = addr_map.len().saturating_sub(1);
    data.put_u32_le(num_users as u32);

    data.put_u32_le(num_games);

    // User list
    for (addr, session_id) in addr_map.iter() {
        if addr != src {
            if let Some(client_info) = id_map.get(session_id) {
                packet_util::put_bytes_with_null(&mut data, &client_info.username);
                data.put_u32_le(client_info.ping);
                data.put_u8(client_info.player_status);
                data.put_u16_le(client_info.user_id);
                data.put_u8(client_info.conn_type);
            }
        }
    }

    // Game list — lock each game independently
    for game_arc in &game_arcs {
        let game_info = game_arc.lock().await;
        packet_util::put_bytes_with_null(&mut data, &game_info.game_name);
        data.put_u32_le(game_info.game_id);
        packet_util::put_bytes_with_null(&mut data, &game_info.emulator_name);
        packet_util::put_bytes_with_null(&mut data, &game_info.owner);
        data.put(format!("{}/{}\0", game_info.num_players, game_info.max_players).as_bytes());
        data.put_u8(game_status_to_byte(game_info.game_status));
    }
    Ok(data.to_vec())
}

fn game_status_to_byte(status: u8) -> u8 {
    match status {
        0 => 0, // Waiting
        1 => 1, // Playing
        2 => 2, // Netsync
        _ => 0, // Default to Waiting
    }
}

pub async fn fetch_client_info(
    src: &std::net::SocketAddr,
    state: &AppState,
) -> color_eyre::Result<(Vec<u8>, Vec<u8>, u8, u16)> {
    match state.get_client(src).await {
        Some(client_info) => Ok((
            client_info.username.clone(),
            client_info.emulator_name.clone(),
            client_info.conn_type,
            client_info.user_id,
        )),
        None => Err(eyre!("Client not found: addr={}", src)),
    }
}

pub async fn fetch_game_info(
    src: &std::net::SocketAddr,
    state: &AppState,
) -> color_eyre::Result<GameInfo> {
    // Retrieve game_id from client information
    let client_info = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found: addr={}", src))?;

    let game_id = client_info
        .game_id
        .ok_or_else(|| eyre!("Game ID not found for client: addr={}", src))?;

    // Retrieve game information using game_id
    state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found: game_id={}", game_id))
}

pub async fn with_client_mut<F, R>(
    state: &AppState,
    src: &std::net::SocketAddr,
    f: F,
) -> color_eyre::Result<R>
where
    F: FnOnce(&mut ClientInfo) -> R,
{
    state
        .update_client(src, |client_info| Ok(f(client_info)))
        .await
}

pub async fn with_game_mut<F, R>(
    state: &AppState,
    src: &std::net::SocketAddr,
    f: F,
) -> color_eyre::Result<R>
where
    F: FnOnce(&mut GameInfo) -> R,
{
    // Retrieve game_id from client information
    let client_info = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let game_id = client_info
        .game_id
        .ok_or_else(|| eyre!("Game ID not found for client"))?;

    // Update game information
    state
        .update_game::<_, R, color_eyre::Report>(game_id, |game_info| Ok(f(game_info)))
        .await
}
