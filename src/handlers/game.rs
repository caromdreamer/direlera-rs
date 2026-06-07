use bytes::{Buf, BytesMut};
use color_eyre::eyre::eyre;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use super::util;
use crate::kaillera::message_types as msg;
use crate::simplest_game_sync;
use crate::*;

// Refactored handle_create_game function
#[tracing::instrument(skip_all)]
pub async fn handle_create_game(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    // Check if user is already in a game
    if let Some(client_info) = state.get_client(src).await {
        if let Some(existing_game_id) = client_info.game_id {
            // Verify the game actually exists and user is still in it
            if let Some(existing_game) = state.get_game(existing_game_id).await {
                if existing_game.players.iter().any(|p| p.addr == *src) {
                    tracing::warn!("User attempted to create game while already in a game");
                    return Ok(()); // Silently ignore invalid request
                }
            }
            // If game doesn't exist or user is not in it, clean up stale game_id
            util::with_client_mut(&state, src, |client_info| {
                client_info.game_id = None;
            })
            .await?;
        }
    }

    // Parse the message to extract game_name
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = util::read_string_bytes(&mut buf); // Empty String
    let mut game_name = util::read_string_bytes(&mut buf); // Game Name (read as bytes to preserve encoding)
    let _ = util::read_string_bytes(&mut buf); // Empty String
    let _ = if buf.len() >= 4 { buf.get_u32_le() } else { 0 }; // 4B: 0xFF

    // Validate game name length (127 bytes max - not characters, to preserve encoding)
    if game_name.len() > 127 {
        warn!(
            "Game name too long ({} bytes), truncating to 127",
            game_name.len()
        );
        // Truncate to 127 bytes
        game_name.truncate(127);
    }

    // Lock-free ID generation!
    let game_id = state.next_game_id();

    // Get client_info
    let (username, emulator_name, conn_type, user_id) =
        util::fetch_client_info(src, &state).await?;

    // Create new game
    let game_info = GameInfo {
        game_id,
        game_name: game_name.clone(),
        emulator_name: emulator_name.clone(),
        owner: username.clone(),
        owner_user_id: user_id, // Store owner's user_id for authorization
        num_players: 1,
        max_players: 4,
        game_status: GAME_STATUS_WAITING,
        sync_manager: None, // Will be initialized when game starts
        players: vec![GamePlayerInfo {
            addr: *src,
            username: username.clone(),
            user_id,
            conn_type,
            last_game_data_recv: None,
            interval_baseline_secs: None,
            left_room: false,
        }],
        metric_labels: Arc::new(GameMetricLabels {
            // Observability-only UUID; the wire game_id sent to clients is unchanged.
            game_uid: uuid::Uuid::new_v4().to_string(),
            game_name: util::sanitize_label(&game_name),
            emulator_name: util::sanitize_label(&emulator_name),
        }),
    };

    // Add game
    state.add_game(game_id, game_info.clone()).await;

    util::with_client_mut(&state, src, |client_info| {
        client_info.game_id = Some(game_id);
        // Game membership changed — update the session span.
        client_info.session_span.record("game_id", game_id);
    })
    .await?;

    info!(
        { fields::GAME_NAME } = util::bytes_for_log(&game_name).as_str(),
        "Game created (emulator {})",
        util::bytes_for_log(&emulator_name)
    );

    // Build data for new game notification
    let data = util::build_new_game_notification(&username, &game_name, &emulator_name, game_id);

    // Broadcast new game notification to all clients
    util::broadcast_packet(&state, msg::CREATE_GAME, data).await?;

    // Send game status update to the client
    let status_data = util::make_update_game_status(&game_info)?;
    util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;

    // Send player information (empty list for the creator)
    let players_info = util::make_player_information(src, &state, &game_info).await?;
    util::send_packet(&state, src, msg::PLAYER_INFORMATION, players_info).await?;

    // Build and send join game response
    let response_data = {
        let client_info = state
            .get_client(src)
            .await
            .ok_or_else(|| eyre!("Client not found"))?;
        util::build_join_game_response(&client_info)
    };
    util::send_packet(&state, src, msg::JOIN_GAME, response_data).await?;
    util::record_processing_time("create_game", start.elapsed());
    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn handle_join_game(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    // Parse message
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = util::read_string_bytes(&mut buf);
    let game_id = buf.get_u32_le();
    let _ = util::read_string_bytes(&mut buf);
    let _ = buf.get_u32_le();
    let _ = buf.get_u16_le();
    let _conn_type = buf.get_u8();

    // Get joining player's connection type
    let client = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let conn_type = client.conn_type;

    // Prevent joining if user is already in any game (same or different)
    if client.game_id.is_some() {
        tracing::warn!(
            "User attempted to join game {} while already in another game",
            game_id
        );
        return Ok(()); // Silently ignore invalid request
    }

    util::with_client_mut(&state, src, |client_info| {
        client_info.game_id = Some(game_id);
        // Game membership changed — update the session span.
        client_info.session_span.record("game_id", game_id);
    })
    .await?;

    let username = client.username.clone();
    let user_id = client.user_id;

    util::with_game_mut(&state, src, |game_info| {
        // Only add if not already in the game (prevents duplicates)
        if !game_info.players.iter().any(|p| p.addr == *src) {
            game_info.num_players += 1;
            game_info.players.push(GamePlayerInfo {
                addr: *src,
                username: username.clone(),
                user_id,
                conn_type,
                last_game_data_recv: None,
                interval_baseline_secs: None,
                left_room: false,
            });
        } else {
            debug!("Player already in game, skipping duplicate");
        }
    })
    .await?;

    // Generate game status data
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;
    let status_data = util::make_update_game_status(&game_info)?;

    info!(
        { fields::PLAYER_COUNT } = game_info.num_players,
        "Player joined game"
    );

    // Broadcast game status update to all clients
    let client_addresses = state.get_all_client_addrs().await;
    for addr in client_addresses {
        util::send_packet(&state, &addr, msg::UPDATE_GAME_STATUS, status_data.clone()).await?;
    }

    // Generate player information and send to joining client
    let players_info = util::make_player_information(src, &state, &game_info).await?;
    util::send_packet(&state, src, msg::PLAYER_INFORMATION, players_info.clone()).await?;

    // Generate join game response data
    let response_data = {
        let client_info = state
            .get_client(src)
            .await
            .ok_or_else(|| eyre!("Client not found"))?;
        util::build_join_game_response(&client_info)
    };

    // Send join game notification to ALL players (including the joining player)
    // Each player manages their own list, so we send the new player info to everyone
    util::broadcast_packet_to_game(&state, game_id, msg::JOIN_GAME, response_data).await?;
    util::record_processing_time("join_game", start.elapsed());
    Ok(())
}

/*
'Quit Game State
'Client: Quit Game Request [0x0B]
'Server: Update Game Status [0x0E]
'Server: Quit Game Notification [0x0B]
'
'Close Game State
'Client: Quit Game Request [0x0B]
'Server: Close Game Notification [0x10]
'Server: Quit Game Notification [0x0B]
'     0x0B = Quit Game
'            Client Request:
'            NB : Empty String [00]
'            2B : 0xFF
'
'            Server Notification:
'            NB : Username
'            2B : UserID

'     0x10 = Close game
'            Server Notification:
'            NB : Empty String [00]
'            4B : GameID
 */
/// If the current owner is no longer an active room member (left the room), hand
/// ownership to the first remaining in-room player. The server enforces owner-gated
/// actions (start/kick) via `owner_user_id`, so no migration packet is required;
/// returns the new owner's username when ownership changed so the caller can
/// announce it in the room. No-op (None) when the owner is still present or the
/// room is empty.
fn migrate_owner_if_needed(game_info: &mut GameInfo) -> Option<Vec<u8>> {
    let owner_present = game_info
        .players
        .iter()
        .any(|p| !p.left_room && p.user_id == game_info.owner_user_id);
    if owner_present {
        return None;
    }
    let p = game_info.players.iter().find(|p| !p.left_room)?;
    let new_owner = p.username.clone();
    let new_owner_id = p.user_id;
    info!(
        "Owner left - migrating ownership to {}",
        util::bytes_for_log(&new_owner)
    );
    game_info.owner = new_owner.clone();
    game_info.owner_user_id = new_owner_id;
    Some(new_owner)
}

#[tracing::instrument(skip_all)]
pub async fn handle_quit_game(
    _message: Vec<u8>,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    // Get client info and validate
    let client_info = match state.get_client(src).await {
        Some(client_info) => client_info,
        None => {
            // Benign teardown race: the session was already cleaned up.
            debug!("Quit game ignored: client not found (post-teardown race)");
            return Ok(());
        }
    };

    let game_id = match client_info.game_id {
        Some(game_id) => game_id,
        None => {
            // Benign teardown race: the game was already closed (e.g. the owner
            // quit first) before this client's quit arrived.
            debug!("Quit game ignored: client no longer in a game (post-teardown race)");
            return Ok(());
        }
    };

    let username = client_info.username.clone();
    let user_id = client_info.user_id;

    let game_arc = match state.get_game_arc(game_id).await {
        Some(arc) => arc,
        None => {
            debug!("Quit game ignored: game already gone (post-teardown race)");
            return Ok(());
        }
    };

    // Return this client to the lobby regardless of game state.
    util::with_client_mut(&state, src, |client_info| {
        client_info.game_id = None;
        client_info.player_status = PLAYER_STATUS_IDLE;
        // Back in the lobby — 0 is the "no game" sentinel (game ids start at 1).
        client_info.session_span.record("game_id", 0u32);
    })
    .await?;

    // Decide and mutate under a single lock so the "is it playing?" check and the
    // Vec mutation can't race a concurrent game-end. The players Vec doubles as the
    // lockstep sync-slot index space, so it must not be reindexed while a game is
    // live (removing a middle player shifts later players' position() and routes
    // their input to the wrong/dropped slot, freezing the game).
    enum QuitOutcome {
        Playing,       // tombstoned; drop in sync, compaction deferred to game end
        WaitingOpen,   // removed; room stays with remaining players
        WaitingClosed, // removed; room now empty
    }
    // `new_owner` carries the new host's username if ownership migrated (to announce).
    let (outcome, new_owner) = {
        let mut game_info = game_arc.lock().await;
        let idx = game_info.players.iter().position(|p| p.addr == *src);
        if game_info.sync_manager.is_some() {
            // Live game: tombstone the slot (keep it), don't remove.
            if let Some(i) = idx {
                game_info.players[i].left_room = true;
            }
            game_info.num_players = game_info.players.iter().filter(|p| !p.left_room).count() as u8;
            let migrated = migrate_owner_if_needed(&mut game_info);
            (QuitOutcome::Playing, migrated)
        } else {
            // No live sync → safe to remove from the Vec immediately.
            if let Some(i) = idx {
                game_info.players.remove(i);
            }
            game_info.num_players = game_info.players.len() as u8;
            let migrated = migrate_owner_if_needed(&mut game_info);
            let outcome = if game_info.players.is_empty() {
                QuitOutcome::WaitingClosed
            } else {
                QuitOutcome::WaitingOpen
            };
            (outcome, migrated)
        }
    };

    let quit_data = packet_util::build_quit_game_packet(&username, user_id);

    match outcome {
        QuitOutcome::Playing => {
            // Drop in the sync engine: keeps the slot (zero-filled) so the remaining
            // players keep advancing, and ends/compacts the game if this was the
            // last active player.
            info!("Player quit during play - dropping slot, keeping game alive");
            let _ = execute_drop_game(game_id, src, &state).await;

            // execute_drop_game may have closed the room (if everyone had left),
            // sending CLOSE_GAME itself.
            if let Some(game_info) = state.get_game(game_id).await {
                util::broadcast_packet_to_game(&state, game_id, msg::QUIT_GAME, quit_data).await?;
                let status_data = util::make_update_game_status(&game_info)?;
                util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;
            } else {
                util::broadcast_packet(&state, msg::QUIT_GAME, quit_data).await?;
            }
        }
        QuitOutcome::WaitingClosed => {
            info!("Last player left waiting room - closing game");
            state.remove_game(game_id).await;
            let close = packet_util::build_close_game_packet(game_id);
            util::broadcast_packet(&state, msg::CLOSE_GAME, close).await?;
            util::broadcast_packet(&state, msg::QUIT_GAME, quit_data).await?;
        }
        QuitOutcome::WaitingOpen => {
            info!("Player quit waiting room");
            util::broadcast_packet_to_game(&state, game_id, msg::QUIT_GAME, quit_data).await?;
            if let Some(game_info) = state.get_game(game_id).await {
                let status_data = util::make_update_game_status(&game_info)?;
                util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;
            }
        }
    }

    // Announce the new host in the room (if ownership migrated and the room still
    // exists). Kaillera has no owner-change packet, so use a server game-chat line.
    if let Some(owner_name) = new_owner {
        if state.get_game(game_id).await.is_some() {
            let text = format!("{} is now the room host", util::bytes_for_log(&owner_name));
            let chat = packet_util::build_game_chat_packet(b"Server", text.as_bytes());
            util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, chat).await?;
        }
    }

    util::record_processing_time("quit_game", start.elapsed());
    Ok(())
}

/*
'     0x11 = Start Game
'            Client Request:
'            NB : Empty String [00]
'            2B : 0xFF
'            1B : 0xFF
'            1B : 0xFF
'
'            Server Notification:
'            NB : Empty String [00]
'            2B : Frame Delay (eg. (connectionType * (frameDelay + 1) <-Block on that frame
'            1B : Your Player Number (eg. if you're player 1 or 2...)
'            1B : Total Players
- **Client**: Sends **Start Game Request** `[0x11]`
- **Server**: Sends **Update Game Status** `[0x0E]`
- **Server**: Sends **Start Game Notification** `[0x11]`
- **Client**: Enters **Netsync Mode** and waits for all players to send **Ready to Play Signal** `[0x15]`
- **Server**: Sends **Update Game Status** for whole server players`[0x0E]`
- **Server**: Enters **Playing Mode** after receiving **Ready to Play Signal Notification** `[0x15]` from all players in room
- **Client(s)**: Exchange data using **Game Data Send** `[0x12]` or **Game Cache Send** `[0x13]`
- **Server**: Sends data accordingly using **Game Data Notify** `[0x12]` or **Game Cache Notify** `[0x13]`

 */
#[tracing::instrument(skip_all)]
pub async fn handle_start_game(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let mut buf = BytesMut::from(&message.data[..]);
    let start = Instant::now();
    let _ = util::read_string_bytes(&mut buf); // Empty String
    let _ = buf.get_u32_le(); // 0xFFFF 0xFF 0xFF

    let client = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let requester_username = client.username.clone();
    let requester_user_id = client.user_id;
    let game_id = client
        .game_id
        .ok_or_else(|| eyre!("Client not in a game"))?;

    // Check if requester is the game owner (using user_id to prevent nickname abuse)
    let game_info = state
        .get_game(game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;

    // Verify requester is actually in the game's players list
    if !game_info.players.iter().any(|p| p.addr == *src) {
        warn!("User attempted to start game but not in game players list");
        return Ok(()); // Silently ignore invalid request
    }

    if game_info.sync_manager.is_some() {
        warn!("Start ignored: game already started");
        let chat_message =
            packet_util::build_game_chat_packet(&requester_username, b"Game is already started");
        util::broadcast_packet_to_game(&state, game_id, msg::GAME_CHAT, chat_message).await?;
        return Ok(()); // Silently ignore invalid request
    }
    if game_info.owner_user_id != requester_user_id {
        warn!(
            "Non-owner attempted to start game (owner user_id {})",
            game_info.owner_user_id
        );
        return Ok(()); // Silently ignore invalid request
    }

    // Defensive: drop any tombstoned (left-the-room) players before sizing the sync
    // engine, so a stray ghost can never get a sync slot in the new game. Normally
    // game-end compaction already cleared these.
    util::with_game_mut(&state, src, |game_info| {
        game_info.players.retain(|p| !p.left_room);
        game_info.num_players = game_info.players.len() as u8;
    })
    .await?;

    // Get game info first to get player list
    let game_info_before = util::fetch_game_info(src, &state).await?;
    let players = game_info_before.players.clone();

    // Initialize CachedGameSync with player delays (derived from conn_type).
    // Each client uses its own conn_type as the frame delay regardless of the
    // server's assignment, so the sync engine must match that packet cadence.
    let delays: Vec<usize> = players.iter().map(|p| p.conn_type as usize).collect();

    info!("Calculated frame delays for game start: {:?}", delays);

    let disable_output_cache = state.config.disable_output_cache;

    // Initialize SimpleGameSync when game starts
    util::with_game_mut(&state, src, |game_info| {
        game_info.game_status = GAME_STATUS_PLAYING;
        game_info.sync_manager = Some(
            simplest_game_sync::CachedGameSync::new(delays.clone())
                .with_output_cache_disabled(disable_output_cache),
        );
    })
    .await?;

    // Update all players' status to NET_SYNC when game starts
    for player in &players {
        util::with_client_mut(&state, &player.addr, |client_info| {
            client_info.player_status = PLAYER_STATUS_NET_SYNC;
        })
        .await?;
    }

    let game_info = util::fetch_game_info(src, &state).await?;

    info!(
        { fields::PLAYER_COUNT } = game_info.players.len(),
        { fields::GAME_STATUS } = "playing",
        "Game started"
    );

    // Update game status
    let status_data = util::make_update_game_status(&game_info)?;
    util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;

    // Broadcast server status update to all clients to reflect player status changes
    // This ensures that all clients see the updated player_status (NET_SYNC/PLAYING) in the server list
    // let all_client_addrs = state.get_all_client_addrs().await;
    // for client_addr in &all_client_addrs {
    //     if let Ok(data) = util::make_server_status(client_addr, &state).await {
    //         util::send_packet(&state, client_addr, msg::SERVER_STATUS, data).await?;
    //     }
    // }

    // Send start game notification with each player's delay
    for (i, player) in game_info.players.iter().enumerate() {
        let player_delay = player.conn_type as usize;
        let player_number = (i + 1) as u8;
        let total_players = game_info.players.len() as u8;
        debug!(
            "Sending start game notification to {} (player {}, frame_delay {})",
            player.addr, player_number, player_delay
        );
        let data =
            packet_util::build_start_game_packet(player_delay as u16, player_number, total_players);
        util::send_packet(&state, &player.addr, msg::START_GAME, data).await?;
    }
    util::record_processing_time("start_game", start.elapsed());
    Ok(())
}

/*
0x14 = Drop Game

This ends the game session but keeps the room open.
Players remain in the room and can start a new game.

Client Request:
- NB : Empty String [00]
- 1B : 0x00

Server Notification:
- NB : Username (who dropped the game)
- 1B : Player Number (which player number dropped)

Flow:
1. Client: Drop Game Request [0x14]
2. Server: Drop Game Notification [0x14] (to all players in the room)
   - All players receive the username and player number of who dropped
3. Server: Update Game Status [0x0E] (game_status = 0: Waiting)

Note: This is different from Quit Game (0x0B) which removes players from the room.
*/

/// Execute drop game logic - ends the game but keeps the room open
/// Returns true if game was actually dropped, false if game was not in playing state
pub async fn execute_drop_game(
    game_id: u32,
    src: &std::net::SocketAddr,
    state: &Arc<AppState>,
) -> color_eyre::Result<bool> {
    // Get client info
    let client = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Client not found"))?;
    let username = client.username.clone();

    // Validate game is playing and get dropper info
    let dropper_player_id = {
        let game_arc = match state.get_game_arc(game_id).await {
            Some(arc) => arc,
            None => {
                debug!("Game not found during drop game, ignoring");
                return Ok(false);
            }
        };
        let game_info = game_arc.lock().await;
        if game_info.game_status != GAME_STATUS_PLAYING {
            info!("Game is not playing, skipping drop game");
            return Ok(false);
        }
        match game_info.players.iter().position(|p| p.addr == *src) {
            Some(player_id) => player_id,
            None => {
                debug!("Dropper not found in game players list, ignoring");
                return Ok(false);
            }
        }
    };

    // Mark player as dropped and get all necessary data
    let (outputs, players, all_dropped) = {
        let game_arc = state
            .get_game_arc(game_id)
            .await
            .ok_or_else(|| eyre!("Game not found"))?;
        let mut game_info = game_arc.lock().await;

        info!("Ending game");

        // Mark player as dropped and get pending outputs
        let outputs = game_info
            .sync_manager
            .as_mut()
            .ok_or_else(|| eyre!("Sync manager not found"))?
            .mark_player_dropped(dropper_player_id)
            .map_err(|e| eyre!("Failed to mark player dropped: {}", e))?;

        info!("Marked player {} as dropped", dropper_player_id);

        let all_dropped = game_info
            .sync_manager
            .as_ref()
            .ok_or_else(|| eyre!("Sync manager not found"))?
            .sync
            .all_players_dropped();

        // Clone the player list BEFORE any compaction: `outputs` are indexed by the
        // live sync slot, so the target lookup below must use the original order.
        let players = game_info.players.clone();

        if all_dropped {
            // Game over → sync goes inactive, so reindexing the Vec is now safe.
            game_info.game_status = GAME_STATUS_WAITING;
            game_info.sync_manager = None;
            // Purge players who left the room mid-game (tombstones); plain drops
            // stay in the room and can start another game.
            game_info.players.retain(|p| !p.left_room);
            game_info.num_players = game_info.players.len() as u8;
            migrate_owner_if_needed(&mut game_info);
            let status_data = util::make_update_game_status(&game_info)?;
            util::broadcast_packet(state, msg::UPDATE_GAME_STATUS, status_data).await?;
        }

        (outputs, players, all_dropped)
    };

    // If the game ended and everyone had left the room (not just dropped), close it
    // rather than leaving an empty room behind.
    if all_dropped {
        let empty = state
            .get_game(game_id)
            .await
            .map(|g| g.players.is_empty())
            .unwrap_or(true);
        if empty {
            info!("All players left during play - closing game");
            state.remove_game(game_id).await;
            let close = packet_util::build_close_game_packet(game_id);
            util::broadcast_packet(state, msg::CLOSE_GAME, close).await?;
            return Ok(true);
        }
    }

    // Update all players' status back to IDLE (waiting in room)
    for player in &players {
        util::with_client_mut(state, &player.addr, |client_info| {
            client_info.player_status = PLAYER_STATUS_IDLE;
        })
        .await?;
    }

    let dropper_player_num = (dropper_player_id + 1) as u8;

    let notification_data = packet_util::build_drop_game_packet(&username, dropper_player_num);
    util::broadcast_packet_to_game(state, game_id, msg::DROP_GAME, notification_data).await?;

    // Send any outputs that can now be sent due to the drop
    for output in outputs {
        // Safety check: ensure player_id is within bounds
        let target_addr = &players
            .get(output.player_id)
            .ok_or_else(|| {
                eyre!(
                    "Invalid player_id: {} (players count: {})",
                    output.player_id,
                    players.len()
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

        info!(
            { fields::MESSAGE_TYPE } = msg::message_type_name(message_type),
            "Sending game data/cache after drop to player {}", output.player_id
        );
        util::send_packet(state, target_addr, message_type, data_to_send).await?;
    }

    info!(
        { fields::PLAYER_COUNT } = players.len(),
        "Game ended, room remains open"
    );

    Ok(true)
}

#[tracing::instrument(skip_all)]
pub async fn handle_drop_game(
    _message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    debug!("Drop game request received");

    let Some((game_id, _player_id)) = util::resolve_in_game(&state, src).await else {
        debug!("Drop game ignored: client no longer in a game (post-teardown race)");
        return Ok(());
    };

    execute_drop_game(game_id, src, &state).await?;
    util::record_processing_time("drop_game", start.elapsed());
    Ok(())
}

/*
 **Client to Server**:
  - Empty String
  - `2B`: UserID
*/
#[tracing::instrument(skip_all)]
pub async fn handle_kick_user(
    message: kaillera::protocol::ParsedMessage,
    src: &std::net::SocketAddr,
    state: Arc<AppState>,
) -> color_eyre::Result<()> {
    let start = Instant::now();
    let mut buf = BytesMut::from(&message.data[..]);
    let _ = util::read_string_bytes(&mut buf); // Empty String
    let user_id = buf.get_u16_le(); // UserID

    // Check if requester is the game owner (using user_id to prevent nickname abuse)
    let requester_info = state
        .get_client(src)
        .await
        .ok_or_else(|| eyre!("Requester not found"))?;
    let requester_user_id = requester_info.user_id;
    let requester_game_id = requester_info
        .game_id
        .ok_or_else(|| eyre!("Requester not in a game"))?;

    let game_info = state
        .get_game(requester_game_id)
        .await
        .ok_or_else(|| eyre!("Game not found"))?;

    // Verify requester is actually in the game's players list
    if !game_info.players.iter().any(|p| p.addr == *src) {
        warn!("User attempted to kick but not in game players list");
        return Ok(()); // Silently ignore invalid request
    }

    if game_info.owner_user_id != requester_user_id {
        warn!(
            "Non-owner attempted to kick user (owner user_id {})",
            game_info.owner_user_id
        );
        return Ok(()); // Silently ignore invalid request
    }

    let (username, client_user_id, client_addr) = {
        let addr_map = state.clients_by_addr.read().await;
        let id_map = state.clients_by_id.read().await;

        let client_info = addr_map.iter().find_map(|(addr, session_id)| {
            let client = id_map.get(session_id)?;
            if client.user_id == user_id {
                Some((addr, client))
            } else {
                None
            }
        });

        match client_info {
            Some((addr, client_info)) => (client_info.username.clone(), client_info.user_id, *addr),
            None => {
                error!(
                    "Client not found during kick user (target user_id {})",
                    user_id
                );
                return Ok(());
            }
        }
    };

    // Verify the kicked user is in the same game as requester
    let game_id = {
        let client_info = state.get_client(&client_addr).await;
        match client_info {
            Some(client_info) => match client_info.game_id {
                Some(game_id) => {
                    if game_id != requester_game_id {
                        warn!(
                            "Attempted to kick user from a different game (target game {})",
                            game_id
                        );
                        return Ok(());
                    }
                    game_id
                }
                None => {
                    error!(
                        "Game ID not found during kick user (target user_id {})",
                        client_user_id
                    );
                    return Ok(());
                }
            },
            None => {
                error!(
                    "Client not found during kick user (target user_id {})",
                    user_id
                );
                return Ok(());
            }
        }
    };

    let game_info_clone = {
        let game_arc = match state.get_game_arc(game_id).await {
            Some(arc) => arc,
            None => {
                error!("Game not found during kick user");
                return Ok(());
            }
        };
        let mut game_info = game_arc.lock().await;
        if let Some(idx) = game_info.players.iter().position(|p| p.addr == client_addr) {
            game_info.players.remove(idx);
            game_info.num_players -= 1;
        }
        game_info.clone()
    };

    info!(
        "Kicked user {} (user_id {}) from game",
        util::bytes_for_log(&username),
        client_user_id
    );

    // Update game status
    let status_data = util::make_update_game_status(&game_info_clone)?;
    util::broadcast_packet(&state, msg::UPDATE_GAME_STATUS, status_data).await?;

    // Quit game notification
    let data = packet_util::build_quit_game_packet(&username, client_user_id);
    util::broadcast_packet_to_game(&state, game_id, msg::QUIT_GAME, data).await?;
    util::record_processing_time("kick_user", start.elapsed());
    Ok(())
}
