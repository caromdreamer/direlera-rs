use bytes::{Buf, BytesMut};
use color_eyre::eyre::eyre;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::util;
use crate::kaillera::message_types as msg;
use crate::simplest_game_sync;
use crate::*;

/// EWMA smoothing factor for the per-player interval baseline. Small enough that
/// transient spikes barely move the baseline, so a real slowdown surfaces as a
/// ratio > 1 instead of being absorbed into "normal".
const INTERVAL_BASELINE_ALPHA: f64 = 0.1;

/// Record input-pacing metrics for a player on every input packet (game_data or
/// game_cache). The pace ratio (current interval / the game's own EWMA baseline)
/// is fps/conn_type/batching-agnostic: 1.0 means on pace, 2.0 means running at
/// half the game's normal speed (a stall/lag). Call inside the per-game lock.
fn record_input_pacing(
    player: &mut GamePlayerInfo,
    now: std::time::Instant,
    labels: &GameMetricLabels,
    player_count: usize,
) {
    if let Some(last_recv) = player.last_game_data_recv {
        let interval = now.duration_since(last_recv).as_secs_f64();
        // Per-game labels so a single bad game can be isolated on a dashboard
        // (one stalling game would otherwise drag the whole aggregate down).
        // game_uid is a restart-safe UUID; series are bounded by the exporter's
        // idle_timeout (main.rs). Label values are precomputed (no hot-path alloc).
        let uid = labels.game_uid.clone();
        let title = labels.game_name.clone();
        let emulator = labels.emulator_name.clone();
        let pc = player_count.to_string();
        // Absolute interval — for an at-a-glance pace view.
        metrics::histogram!(
            "client_input_interval_seconds",
            "game_uid" => uid.clone(),
            "game_name" => title.clone(),
            "emulator_name" => emulator.clone(),
            "player_count" => pc.clone(),
        )
        .record(interval);
        match player.interval_baseline_secs {
            Some(baseline) if baseline > 0.0 => {
                metrics::histogram!(
                    "client_input_pace_ratio",
                    "game_uid" => uid,
                    "game_name" => title,
                    "emulator_name" => emulator,
                    "player_count" => pc,
                )
                .record(interval / baseline);
                player.interval_baseline_secs = Some(
                    INTERVAL_BASELINE_ALPHA * interval + (1.0 - INTERVAL_BASELINE_ALPHA) * baseline,
                );
            }
            // First interval just seeds the baseline; no ratio emitted yet.
            _ => player.interval_baseline_secs = Some(interval),
        }
    }
    player.last_game_data_recv = Some(now);
}

/*
- **NB**: Empty String `[00]`
- **2B**: Length of Game Data
- **NB**: Game Data
 */
#[tracing::instrument(level = "debug", skip_all, fields(
    player_id = tracing::field::Empty,
))]
pub async fn handle_game_data(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = buf.get_u8(); // Empty String
    let data_length = buf.get_u16_le() as usize;
    let game_data = buf.split_to(data_length).to_vec();

    let client = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let game_id = client.game_id.ok_or_else(|| eyre!("Game ID not found"))?;

    // Find player_id from address
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    let player_id = game_info
        .players
        .iter()
        .position(|p| p.addr == *src)
        .ok_or_else(|| eyre!("Player not in game"))?;
    tracing::Span::current().record("player_id", player_id);

    // Process with SimpleGameSync (per-game lock — does not block other games)
    let (outputs, cache_overflowed, cache_milestone) = state
        .update_game(game_id, |game_info| {
            // Input pacing: how fast this player's inputs arrive vs the game's
            // own steady-state pace (fps/conn_type-agnostic stall signal).
            let now = Instant::now();
            let player_count = game_info.players.len();
            let labels = game_info.metric_labels.clone();
            if let Some(player) = game_info.players.get_mut(player_id) {
                record_input_pacing(player, now, &labels, player_count);
            }

            let sync_manager = game_info
                .sync_manager
                .as_mut()
                .ok_or_else(|| eyre!("SimpleGameSync not initialized"))?;
            sync_manager
                .process_client_input(
                    player_id,
                    simplest_game_sync::ClientInput::GameData(game_data),
                )
                .map_err(|e| eyre!("Game sync error: {}", e))
        })
        .await?;

    if let Some(n) = cache_milestone {
        let msg_text = format!("[Debug] cache {}/256", n);
        let data = crate::packet_util::build_game_chat_packet(b"Server", msg_text.as_bytes());
        util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, data).await?;
    }
    if cache_overflowed {
        let data =
            crate::packet_util::build_game_chat_packet(b"Server", b"[Debug] cache evicted (256+)");
        util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, data).await?;
    }

    // Send outputs to respective players
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    for output in outputs {
        // Safety check: ensure player_id is within bounds
        let target_addr = game_info
            .players
            .get(output.player_id)
            .ok_or_else(|| {
                eyre!(
                    "Invalid player_id: {} (players count: {})",
                    output.player_id,
                    game_info.players.len()
                )
            })?
            .addr;

        let (message_type, data_to_send) = match output.response {
            simplest_game_sync::ServerResponse::GameData(data) => {
                (msg::GAME_DATA, packet_util::build_game_data_packet(&data))
            }
            simplest_game_sync::ServerResponse::GameCache(position) => (
                msg::GAME_CACHE,
                packet_util::build_game_cache_packet(position),
            ),
        };

        util::send_packet(&state, &target_addr, message_type, data_to_send).await?;
    }

    metrics::histogram!(
        "game_sync_processing_seconds",
        "type" => "game_data",
        "player_count" => game_info.players.len().to_string(),
    )
    .record(start.elapsed().as_secs_f64());

    Ok(())
}

#[tracing::instrument(level = "debug", skip_all, fields(
    player_id = tracing::field::Empty,
))]
pub async fn handle_game_cache(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = buf.get_u8(); // Empty String
    let cache_position = buf.get_u8();

    let client = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let game_id = client.game_id.ok_or_else(|| eyre!("Game ID not found"))?;

    // Find player_id from address
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    let player_id = game_info
        .players
        .iter()
        .position(|p| p.addr == *src)
        .ok_or_else(|| eyre!("Player not in game"))?;
    tracing::Span::current().record("player_id", player_id);

    // Process with SimpleGameSync. Return GameSyncError directly so we can inspect
    // the variant before converting to eyre (cache-miss needs a client notification).
    let sync_result: Result<_, simplest_game_sync::GameSyncError> = state
        .update_game(game_id, |game_info| {
            // Cache packets are the bulk of steady-state traffic, so they must
            // feed the same pacing metric as game_data (else most of the game
            // is invisible to the responsiveness signal).
            let now = Instant::now();
            let player_count = game_info.players.len();
            let labels = game_info.metric_labels.clone();
            if let Some(player) = game_info.players.get_mut(player_id) {
                record_input_pacing(player, now, &labels, player_count);
            }
            let sync_manager = game_info.sync_manager.as_mut().ok_or(
                simplest_game_sync::GameSyncError::BufferInconsistency {
                    message: "sync_manager not initialized".into(),
                },
            )?;
            sync_manager.process_client_input(
                player_id,
                simplest_game_sync::ClientInput::GameCache(cache_position),
            )
        })
        .await;

    let (outputs, cache_overflowed, cache_milestone) = match sync_result {
        Ok(outputs) => outputs,
        Err(simplest_game_sync::GameSyncError::CachePositionNotFound {
            player_id,
            position,
        }) => {
            let data = packet_util::build_game_chat_packet(
                b"Server",
                b"Game Data Error! Game state will be inconsistent!",
            );
            util::send_packet(&state, src, msg::GAME_CHAT, data).await?;
            return Err(eyre!(
                "Cache miss: player {} position {} not found",
                player_id,
                position
            ));
        }
        Err(e) => return Err(eyre!("Game sync error: {}", e)),
    };

    if let Some(n) = cache_milestone {
        let msg_text = format!("[Debug] cache {}/256", n);
        let data = crate::packet_util::build_game_chat_packet(b"Server", msg_text.as_bytes());
        util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, data).await?;
    }
    if cache_overflowed {
        let data =
            crate::packet_util::build_game_chat_packet(b"Server", b"[Debug] cache evicted (256+)");
        util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, data).await?;
    }

    // Send outputs to respective players
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    for output in outputs {
        // Safety check: ensure player_id is within bounds
        let target_addr = game_info
            .players
            .get(output.player_id)
            .ok_or_else(|| {
                eyre!(
                    "Invalid player_id: {} (players count: {})",
                    output.player_id,
                    game_info.players.len()
                )
            })?
            .addr;

        let (message_type, data_to_send) = match output.response {
            simplest_game_sync::ServerResponse::GameData(data) => {
                (msg::GAME_DATA, packet_util::build_game_data_packet(&data))
            }
            simplest_game_sync::ServerResponse::GameCache(position) => (
                msg::GAME_CACHE,
                packet_util::build_game_cache_packet(position),
            ),
        };

        util::send_packet(&state, &target_addr, message_type, data_to_send).await?;
    }

    metrics::histogram!(
        "game_sync_processing_seconds",
        "type" => "game_data",
        "player_count" => game_info.players.len().to_string(),
    )
    .record(start.elapsed().as_secs_f64());

    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn handle_ready_to_play_signal(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    use tracing::info;
    debug!("Ready to play signal received");
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = buf.get_u8(); // Empty String

    state
        .update_client(src, |client_info| {
            client_info.player_status = PLAYER_STATUS_NET_SYNC; // Ready to play
            Ok(())
        })
        .await?;

    let game_info_clone = util::fetch_game_info(src, &state).await?;

    // Update game status
    {
        let status_data = util::make_update_game_status(&game_info_clone)?;
        util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;
    }

    // Check if all users are ready
    let all_user_ready_to_signal = {
        let addr_map = state.clients_by_addr.read().await;
        let id_map = state.clients_by_id.read().await;

        let all_ready = game_info_clone.players.iter().all(|player| {
            if let Some(session_id) = addr_map.get(&player.addr) {
                if let Some(client_info) = id_map.get(session_id) {
                    debug!(
                        "Checking player status of {}: {}",
                        player.addr, client_info.player_status
                    );
                    return client_info.player_status == PLAYER_STATUS_NET_SYNC;
                }
            }
            debug!("Client info not found for {}", player.addr);
            false
        });
        all_ready
    };

    // If all ready, update all players' status
    if all_user_ready_to_signal {
        for player in &game_info_clone.players {
            let _ = state
                .update_client(&player.addr, |client_info| {
                    client_info.player_status = PLAYER_STATUS_PLAYING;
                    Ok(())
                })
                .await;
        }
    }

    // Send ready to play signal notification
    if all_user_ready_to_signal {
        info!(
            { fields::PLAYER_COUNT } = game_info_clone.players.len(),
            "All users ready to signal - starting game"
        );
        let data = packet_util::build_ready_to_play_packet();
        util::broadcast_packet_to_game(&state, game_info_clone.game_id, msg::READY_TO_PLAY, data)
            .await?;
    }
    util::record_processing_time("ready_to_play", start.elapsed());
    Ok(())
}
