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
type TargetOutput = (std::net::SocketAddr, simplest_game_sync::ServerResponse);

/// Record input-pacing metrics for a player on every input packet (game_data or
/// game_cache). The pace ratio (current interval / the game's own EWMA baseline)
/// is fps/conn_type/batching-agnostic: 1.0 means on pace, 2.0 means running at
/// half the game's normal speed (a stall/lag). Call inside the per-game lock.
fn record_input_pacing(
    player: &mut GamePlayerInfo,
    now: std::time::Instant,
    handles: Option<&GameMetricHandles>,
) {
    if let Some(last_recv) = player.last_game_data_recv {
        let interval = now.duration_since(last_recv).as_secs_f64();
        if let Some(handles) = handles {
            // Absolute interval — for an at-a-glance pace view.
            handles.input_interval.record(interval);
        }
        match player.interval_baseline_secs {
            Some(baseline) if baseline > 0.0 => {
                if let Some(handles) = handles {
                    handles.input_pace_ratio.record(interval / baseline);
                }
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

    let Some((game_id, player_id)) = util::resolve_in_game(&state, src).await else {
        debug!("Dropping game_data: client no longer in a game (post-teardown race)");
        return Ok(());
    };
    tracing::Span::current().record("player_id", player_id);

    // Process with SimpleGameSync (per-game lock — does not block other games).
    // Resolve output target addresses while holding the same game lock so the
    // hot path does not clone the whole GameInfo/sync_manager just to send.
    let (target_outputs, metric_handles) = state
        .update_game(
            game_id,
            |game_info| -> color_eyre::Result<(Vec<TargetOutput>, Option<Arc<GameMetricHandles>>)> {
                // Input pacing: how fast this player's inputs arrive vs the game's
                // own steady-state pace (fps/conn_type-agnostic stall signal).
                let now = Instant::now();
                let metric_handles = game_info.metric_handles.clone();
                if let Some(player) = game_info.players.get_mut(player_id) {
                    record_input_pacing(player, now, metric_handles.as_deref());
                }

                let sync_manager = game_info
                    .sync_manager
                    .as_mut()
                    .ok_or_else(|| eyre!("SimpleGameSync not initialized"))?;
                let (outputs, _cache_overflowed, _cache_milestone) = sync_manager
                    .process_client_input(
                        player_id,
                        simplest_game_sync::ClientInput::GameData(game_data),
                    )
                    .map_err(|e| eyre!("Game sync error: {}", e))?;

                let target_outputs = outputs
                    .into_iter()
                    .map(|output| {
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
                        Ok((target_addr, output.response))
                    })
                    .collect::<color_eyre::Result<Vec<_>>>()?;

                Ok((target_outputs, metric_handles))
            },
        )
        .await?;

    // Send outputs to respective players
    for (target_addr, response) in target_outputs {
        let (message_type, data_to_send) = match response {
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

    if let Some(handles) = metric_handles {
        handles.game_data_processing.record(start.elapsed());
    }

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

    let Some((game_id, player_id)) = util::resolve_in_game(&state, src).await else {
        debug!("Dropping game_cache: client no longer in a game (post-teardown race)");
        return Ok(());
    };
    tracing::Span::current().record("player_id", player_id);

    // Process with SimpleGameSync. Return GameSyncError directly so we can inspect
    // the variant before converting to eyre (cache-miss needs a client notification).
    let sync_result: Result<_, simplest_game_sync::GameSyncError> = state
        .update_game(
            game_id,
            |game_info| -> Result<
                (Vec<TargetOutput>, Option<Arc<GameMetricHandles>>),
                simplest_game_sync::GameSyncError,
            > {
                // Cache packets are the bulk of steady-state traffic, so they must
                // feed the same pacing metric as game_data (else most of the game
                // is invisible to the responsiveness signal).
                let now = Instant::now();
                let metric_handles = game_info.metric_handles.clone();
                if let Some(player) = game_info.players.get_mut(player_id) {
                    record_input_pacing(player, now, metric_handles.as_deref());
                }
                let sync_manager = game_info.sync_manager.as_mut().ok_or(
                    simplest_game_sync::GameSyncError::BufferInconsistency {
                        message: "sync_manager not initialized".into(),
                    },
                )?;
                let (outputs, _cache_overflowed, _cache_milestone) = sync_manager
                    .process_client_input(
                        player_id,
                        simplest_game_sync::ClientInput::GameCache(cache_position),
                    )?;

                let target_outputs = outputs
                    .into_iter()
                    .map(|output| {
                        let target_addr =
                            game_info
                                .players
                                .get(output.player_id)
                                .map(|p| p.addr)
                                .ok_or(simplest_game_sync::GameSyncError::InvalidPlayerId {
                                    player_id: output.player_id,
                                    player_count: game_info.players.len(),
                                })?;
                        Ok((target_addr, output.response))
                    })
                    .collect::<Result<Vec<_>, simplest_game_sync::GameSyncError>>()?;

                Ok((target_outputs, metric_handles))
            },
        )
        .await;

    let (target_outputs, metric_handles) = match sync_result {
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

    // Send outputs to respective players
    for (target_addr, response) in target_outputs {
        let (message_type, data_to_send) = match response {
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

    if let Some(handles) = metric_handles {
        handles.game_cache_processing.record(start.elapsed());
    }

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

    let already_playing = state
        .update_client(src, |client_info| {
            let already_playing = client_info.player_status == PLAYER_STATUS_PLAYING;
            if !already_playing {
                client_info.player_status = PLAYER_STATUS_NET_SYNC; // Ready to play
            }
            Ok(already_playing)
        })
        .await?;
    if already_playing {
        debug!("Ready to play ignored: player is already playing");
        return Ok(());
    }

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
