// Simple Game Sync - Cleaner implementation with per-player send buffers
// Each player has their own independent send buffer

#![allow(dead_code)]

// logic description

use std::collections::{HashMap, VecDeque};

use tracing::{error, warn};

type InputData = Vec<u8>;
type OneInput = Vec<u8>;

/// Client input message type
#[derive(Debug, Clone, PartialEq)]
pub enum ClientInput {
    /// Game Data: contains the actual input bytes
    GameData(Vec<u8>),
    /// Game Cache: references a position in the client's cache
    GameCache(u8),
}

/// Server response message type
#[derive(Debug, Clone, PartialEq)]
pub enum ServerResponse {
    /// Game Data: contains the full combined input bytes
    GameData(Vec<u8>),
    /// Game Cache: references a position in the server's cache
    GameCache(u8),
}

/// FIFO cache with 256 slots (rolls over to 0 when full)
#[derive(Debug, Clone)]
pub struct InputCache {
    slots: VecDeque<Vec<u8>>,
    /// Maps data content → absolute indices where that data lives.
    /// Multiple entries exist when the same data appears more than once.
    index_map: HashMap<Vec<u8>, VecDeque<usize>>,
    /// Monotonically increasing counter; next slot gets this index.
    abs_tail: usize,
}

impl Default for InputCache {
    fn default() -> Self {
        Self::new()
    }
}

impl InputCache {
    pub fn new() -> Self {
        Self {
            slots: VecDeque::with_capacity(256),
            index_map: HashMap::new(),
            abs_tail: 0,
        }
    }

    /// Find data in cache, returning the logical position if found. O(1).
    pub fn find(&self, data: &[u8]) -> Option<u8> {
        let indices = self.index_map.get(data)?;
        let &abs_last = indices.back()?;
        let head = self.abs_tail - self.slots.len();
        if abs_last >= head {
            Some((abs_last % 256) as u8)
        } else {
            None
        }
    }

    /// Add data to cache (evicts oldest slot when full). Returns true if eviction occurred.
    pub fn push(&mut self, data: Vec<u8>) -> bool {
        let evicted = if self.slots.len() >= 256 {
            // KNOWN BUG: after this eviction the server's VecDeque-relative positions
            // (0 = oldest) diverge from the client's circular-buffer positions
            // (abs_index % 256). All subsequent cache hits will reference wrong frames,
            // causing game desyncs. This counter lets us observe when it happens.
            metrics::counter!("game_cache_overflow_total").increment(1);
            if self.abs_tail == 256 {
                // first eviction ever for this cache instance
                error!(
                    abs_tail = self.abs_tail,
                    "Game cache hit 256 entries and is now evicting — \
                     cache positions will desync from client (known bug)"
                );
            }
            let evicted = self.slots.pop_front().unwrap();
            if let Some(indices) = self.index_map.get_mut(&evicted) {
                indices.pop_front();
                if indices.is_empty() {
                    self.index_map.remove(&evicted);
                }
            }
            true
        } else {
            false
        };
        self.index_map
            .entry(data.clone())
            .or_default()
            .push_back(self.abs_tail);
        self.abs_tail += 1;
        self.slots.push_back(data);
        evicted
    }

    /// Get data at logical position.
    pub fn get(&self, pos: u8) -> Option<&[u8]> {
        self.slots.get(pos as usize).map(|v| v.as_slice())
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

/// Per-player input state
#[derive(Debug, Clone)]
struct PlayerInput {
    /// Input frames (2-byte chunks)
    frames: Vec<Vec<u8>>,
    /// Client's input cache
    client_cache: InputCache,
    /// Expected input size (delay * 2)
    input_size: usize,
    /// Delay value
    delay: usize,
    /// Number of frames already distributed to send buffers
    distributed_count: usize,
}

impl PlayerInput {
    fn new(delay: usize) -> Self {
        Self {
            frames: Vec::new(),
            client_cache: InputCache::new(),
            input_size: delay * 2,
            delay,
            distributed_count: 0,
        }
    }

    /// Add input (splits into 2-byte chunks)
    fn add_input(&mut self, data: Vec<u8>) {
        for chunk in data.chunks(2) {
            if chunk.len() == 2 {
                self.frames.push(chunk.to_vec());
            }
        }
    }
}

/// Per-player output state
#[derive(Debug, Clone)]
struct PlayerOutputState {
    /// Send buffer: holds frames to send to this player
    /// Each sub-vec is for one source player's frames
    send_buffers: Vec<VecDeque<Vec<u8>>>,
    /// Output cache (combined data this player has received)
    output_cache: InputCache,
    /// Delay value
    delay: usize,
}

impl PlayerOutputState {
    fn new(player_count: usize, delay: usize) -> Self {
        Self {
            send_buffers: (0..player_count).map(|_| VecDeque::new()).collect(),
            output_cache: InputCache::new(),
            delay,
        }
    }

    /// Check if ready to send
    fn can_send(&self) -> bool {
        self.send_buffers.iter().all(|buf| buf.len() >= self.delay)
    }

    /// Extract and combine frames
    fn extract_combined(&mut self) -> Vec<u8> {
        let mut combined = Vec::new();
        for _ in 0..self.delay {
            for buf in &mut self.send_buffers {
                if let Some(frame) = buf.pop_front() {
                    combined.extend_from_slice(&frame);
                }
            }
        }
        combined
    }
}

/// Error types for game sync operations
#[derive(Debug, Clone, PartialEq)]
pub enum GameSyncError {
    /// Invalid player ID (out of range)
    InvalidPlayerId {
        player_id: usize,
        player_count: usize,
    },
    /// Cache position not found
    CachePositionNotFound { player_id: usize, position: u8 },
    /// Internal buffer inconsistency (should not happen in normal operation)
    BufferInconsistency { message: String },
}

impl std::fmt::Display for GameSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GameSyncError::InvalidPlayerId {
                player_id,
                player_count,
            } => write!(
                f,
                "Invalid player_id: {} (valid range: 0..{})",
                player_id, player_count
            ),
            GameSyncError::CachePositionNotFound {
                player_id,
                position,
            } => {
                write!(
                    f,
                    "Cache position {} not found for player {}",
                    position, player_id
                )
            }
            GameSyncError::BufferInconsistency { message } => {
                write!(f, "Buffer inconsistency: {}", message)
            }
        }
    }
}

impl std::error::Error for GameSyncError {}

/// Output action for a specific player with cache support
#[derive(Debug, Clone, PartialEq)]
pub struct CachedPlayerOutput {
    pub player_id: usize,
    pub response: ServerResponse,
}
#[derive(Debug, Clone)]
pub struct SimplestGameSync {
    player_input: Vec<VecDeque<OneInput>>,
    sender_buffer: Vec<VecDeque<OneInput>>,
    player_delays: Vec<usize>,
    dropped_players: Vec<bool>,
    game_data_size: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PlayerOutput {
    pub player_id: usize,
    pub output: Vec<u8>,
}

impl SimplestGameSync {
    pub fn new(player_delays: Vec<usize>) -> Self {
        let player_count = player_delays.len();
        Self {
            player_input: vec![VecDeque::new(); player_count],
            sender_buffer: vec![VecDeque::new(); player_count],
            player_delays,
            dropped_players: vec![false; player_count],
            game_data_size: 0,
        }
    }

    pub fn process_client_input(
        &mut self,
        player_id: usize,
        input: InputData,
    ) -> Result<Vec<PlayerOutput>, GameSyncError> {
        // Validate player_id
        if player_id >= self.player_input.len() {
            return Err(GameSyncError::InvalidPlayerId {
                player_id,
                player_count: self.player_input.len(),
            });
        }

        // Drop된 플레이어가 입력을 보내면 처리하지 않음
        if self.dropped_players[player_id] {
            warn!("Player {} is dropped, skipping input", player_id);
            return Ok(Vec::new());
        }

        // 입력을 delay로 나눠서 청크 생성 (2바이트 제약 제거!)
        let delay = self.player_delays[player_id];
        if delay > 0 && !input.is_empty() {
            let chunk_size = input.len() / delay;
            if chunk_size > 0 {
                // 첫 입력에서 game_data_size 설정
                if self.game_data_size == 0 {
                    self.game_data_size = chunk_size;
                }

                for chunk in input.chunks(chunk_size) {
                    self.player_input[player_id].push_back(chunk.to_vec());
                }
            }
        }

        // Drain ready inputs and collect outputs
        let results = self.drain_ready_inputs()?;

        Ok(results)
    }

    /// Get player delays (for wrapper layer access)
    pub(crate) fn player_delays(&self) -> &[usize] {
        &self.player_delays
    }

    /// Drain ready inputs (all players have input or are dropped) and process them.
    ///
    /// Capped at one "message worth" (max_delay frames) per call. This matches
    /// EmuLinker-K's one-distribution-per-addData pacing: on lag recovery a player
    /// sends catch-up packets rapidly, each triggering one cycle here, so the backlog
    /// drains at the same real-world speed but output is spread across packets instead
    /// of bursting all at once.
    pub fn drain_ready_inputs(&mut self) -> Result<Vec<PlayerOutput>, GameSyncError> {
        let mut results = Vec::new();
        let players = self.player_delays.len();
        let max_frames = self.player_delays.iter().copied().max().unwrap_or(1);
        let mut frames_processed = 0;

        loop {
            if frames_processed >= max_frames {
                break;
            }

            let all_ready = self
                .player_input
                .iter()
                .enumerate()
                .all(|(i, buffer)| !buffer.is_empty() || self.dropped_players[i]);
            let has_any_input = self.player_input.iter().any(|buffer| !buffer.is_empty());

            if !all_ready || !has_any_input {
                break;
            }

            let extract_inputs: Vec<OneInput> = self
                .player_input
                .iter_mut()
                .enumerate()
                .filter_map(|(i, q)| {
                    q.pop_front().or_else(|| {
                        if self.dropped_players[i] {
                            Some(vec![0u8; self.game_data_size])
                        } else {
                            None
                        }
                    })
                })
                .collect();

            frames_processed += 1;

            for buffer in &mut self.sender_buffer {
                buffer.extend(extract_inputs.clone());
            }

            for (pid, buffer) in self.sender_buffer.iter_mut().enumerate() {
                let require_len = self.player_delays[pid] * players;
                while buffer.len() >= require_len {
                    let output: Vec<u8> = buffer.drain(..require_len).flatten().collect();
                    results.push(PlayerOutput {
                        player_id: pid,
                        output,
                    });
                }
            }
        }

        Ok(results)
    }

    /// Mark a player as dropped and drain any ready inputs
    /// Returns any outputs that can now be sent due to the drop
    pub fn mark_player_dropped(
        &mut self,
        player_id: usize,
    ) -> Result<Vec<PlayerOutput>, GameSyncError> {
        if player_id >= self.dropped_players.len() {
            return Err(GameSyncError::InvalidPlayerId {
                player_id,
                player_count: self.dropped_players.len(),
            });
        }
        println!("Marking player {} as dropped", player_id);
        self.dropped_players[player_id] = true;
        self.drain_ready_inputs()
    }

    /// Check if a player is dropped
    pub fn is_player_dropped(&self, player_id: usize) -> bool {
        player_id < self.dropped_players.len() && self.dropped_players[player_id]
    }

    /// Check if all players are dropped
    pub fn all_players_dropped(&self) -> bool {
        self.dropped_players.iter().all(|&dropped| dropped)
    }
}

/// Wrapper layer that adds GameCache support to SimplestGameSync
#[derive(Debug, Clone)]
pub struct CachedGameSync {
    /// Core sync engine without cache
    pub sync: SimplestGameSync,
    /// Per-player input caches (client-side cache)
    input_caches: Vec<InputCache>,
    /// Per-player output caches (server-side cache)
    output_caches: Vec<InputCache>,
}

impl CachedGameSync {
    /// Create a new cached game sync manager
    pub fn new(player_delays: Vec<usize>) -> Self {
        let player_count = player_delays.len();
        Self {
            sync: SimplestGameSync::new(player_delays.clone()),
            input_caches: (0..player_count).map(|_| InputCache::new()).collect(),
            output_caches: (0..player_count).map(|_| InputCache::new()).collect(),
        }
    }

    /// Process client input with cache support.
    /// Returns (outputs, cache_overflowed, milestone) where:
    /// - cache_overflowed: true if any output cache eviction occurred
    /// - milestone: Some(n) if any output cache crossed a 64-entry boundary this call
    pub fn process_client_input(
        &mut self,
        player_id: usize,
        input: ClientInput,
    ) -> Result<(Vec<CachedPlayerOutput>, bool, Option<usize>), GameSyncError> {
        // Validate player_id
        let player_count = self.sync.player_delays().len();
        if player_id >= player_count {
            return Err(GameSyncError::InvalidPlayerId {
                player_id,
                player_count,
            });
        }

        // Resolve input data from cache if needed
        let input_data = match input {
            ClientInput::GameData(data) => {
                self.input_caches[player_id].push(data.clone());
                data
            }
            ClientInput::GameCache(pos) => self.input_caches[player_id]
                .get(pos)
                .ok_or(GameSyncError::CachePositionNotFound {
                    player_id,
                    position: pos,
                })?
                .to_vec(),
        };

        // Process with core sync engine
        let raw_outputs = self.sync.process_client_input(player_id, input_data)?;

        // Convert outputs to cached responses
        let mut results = Vec::new();
        let mut cache_overflowed = false;
        let mut milestone: Option<usize> = None;
        for raw_output in raw_outputs {
            let cached_output = if let Some(cache_pos) =
                self.output_caches[raw_output.player_id].find(&raw_output.output)
            {
                ServerResponse::GameCache(cache_pos)
            } else {
                let before = self.output_caches[raw_output.player_id].len();
                let overflowed =
                    self.output_caches[raw_output.player_id].push(raw_output.output.clone());
                let after = self.output_caches[raw_output.player_id].len();
                if overflowed {
                    cache_overflowed = true;
                }
                // Check if we crossed a 64-entry boundary
                if before / 64 != after / 64 {
                    milestone = Some(after);
                }
                ServerResponse::GameData(raw_output.output)
            };

            results.push(CachedPlayerOutput {
                player_id: raw_output.player_id,
                response: cached_output,
            });
        }

        Ok((results, cache_overflowed, milestone))
    }

    /// Get player count
    pub fn player_count(&self) -> usize {
        self.sync.player_delays().len()
    }

    /// Get player delay
    pub fn get_player_delay(&self, player_id: usize) -> usize {
        self.sync.player_delays()[player_id]
    }

    /// Mark a player as dropped and drain any ready inputs
    /// Returns any outputs that can now be sent due to the drop
    pub fn mark_player_dropped(
        &mut self,
        player_id: usize,
    ) -> Result<Vec<CachedPlayerOutput>, GameSyncError> {
        let raw_outputs = self.sync.mark_player_dropped(player_id)?;

        // Convert outputs to cached responses
        let mut results = Vec::new();
        for raw_output in raw_outputs {
            let cached_output = if let Some(cache_pos) =
                self.output_caches[raw_output.player_id].find(&raw_output.output)
            {
                // Found in cache - use cache reference
                ServerResponse::GameCache(cache_pos)
            } else {
                // Not in cache - store and return full data
                self.output_caches[raw_output.player_id].push(raw_output.output.clone());
                ServerResponse::GameData(raw_output.output)
            };

            results.push(CachedPlayerOutput {
                player_id: raw_output.player_id,
                response: cached_output,
            });
        }

        Ok(results)
    }

    /// Check if a player is dropped
    pub fn is_player_dropped(&self, player_id: usize) -> bool {
        self.sync.is_player_dropped(player_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equal_delays() {
        let mut manager = CachedGameSync::new(vec![1, 1]);

        // Frame 1: P0 sends input
        let outputs = manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        assert_eq!(outputs.0.len(), 0);

        // Frame 1: P1 sends input
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x03, 0x04]))
            .unwrap();
        assert_eq!(outputs.0.len(), 2);

        assert_eq!(outputs.0[0].player_id, 0);
        assert_eq!(outputs.0[1].player_id, 1);

        match &outputs.0[0].response {
            ServerResponse::GameData(data) => {
                assert_eq!(data, &vec![0x01, 0x02, 0x03, 0x04]);
            }
            _ => panic!("P0's first output should be GameData"),
        }

        match &outputs.0[1].response {
            ServerResponse::GameData(data) => {
                assert_eq!(data, &vec![0x01, 0x02, 0x03, 0x04]);
            }
            _ => panic!("P1's first output should be GameData"),
        }

        // Frame 2: Both send same inputs via cache
        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();

        assert_eq!(outputs.0.len(), 2);
        assert!(matches!(
            outputs.0[0].response,
            ServerResponse::GameCache(_)
        ));
        assert!(matches!(
            outputs.0[1].response,
            ServerResponse::GameCache(_)
        ));
    }
    #[test]
    fn test_equal_delays_drop() {
        let mut manager = CachedGameSync::new(vec![1, 1]);
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02, 0x03]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x04, 0x05, 0x06]))
            .unwrap();
        assert_eq!(
            outputs.0,
            vec![
                CachedPlayerOutput {
                    player_id: 0,
                    response: ServerResponse::GameData(vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
                },
                CachedPlayerOutput {
                    player_id: 1,
                    response: ServerResponse::GameData(vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
                },
            ]
        )
    }

    #[test]
    fn test_different_delays() {
        let mut manager = CachedGameSync::new(vec![1, 2]);

        // P0 sends first input
        let outputs = manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x00]))
            .unwrap();
        assert_eq!(outputs.0.len(), 0);

        // P0 sends second and third
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x02, 0x00]))
            .unwrap();
        let outputs = manager
            .process_client_input(0, ClientInput::GameData(vec![0x03, 0x00]))
            .unwrap();
        assert_eq!(outputs.0.len(), 0);

        // P1 sends 4 bytes (2 frames)
        // When P1 sends input, bundles are created from accumulated P0 frames
        // P0 needs: delay 1 * 2 players * 2 bytes = 4 bytes
        // P1 needs: delay 2 * 2 players * 2 bytes = 8 bytes
        // Bundle size: 2 players * 2 bytes = 4 bytes per bundle
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0xAA, 0xBB, 0xCC, 0xDD]))
            .unwrap();

        // Check that we got outputs
        // With the fixed logic:
        // - Bundle 1: [0x01, 0x00] + [0xAA, 0xBB] → P0 gets output immediately (4 bytes >= 4)
        // - Bundle 2: [0x02, 0x00] + [0xCC, 0xDD] → P0 gets another output (4 bytes >= 4), P1 gets output (8 bytes >= 8)
        let p0_outputs: Vec<_> = outputs.0.iter().filter(|o| o.player_id == 0).collect();
        let p1_outputs: Vec<_> = outputs.0.iter().filter(|o| o.player_id == 1).collect();

        // P0 should get 2 outputs (one after each bundle, since delay 1 needs 4 bytes = 1 bundle)
        assert_eq!(
            p0_outputs.len(),
            2,
            "P0 should get 2 outputs (one per bundle)"
        );

        // Verify P0's first output (after Bundle 1)
        match &p0_outputs[0].response {
            ServerResponse::GameData(data) => {
                assert_eq!(
                    data,
                    &vec![0x01, 0x00, 0xAA, 0xBB],
                    "P0's first output should be Bundle 1"
                );
            }
            _ => panic!("P0's first output should be GameData"),
        }

        // Verify P0's second output (after Bundle 2)
        match &p0_outputs[1].response {
            ServerResponse::GameData(data) => {
                assert_eq!(
                    data,
                    &vec![0x02, 0x00, 0xCC, 0xDD],
                    "P0's second output should be Bundle 2"
                );
            }
            _ => panic!("P0's second output should be GameData"),
        }

        // P1 should get 1 output (after 2 bundles, since delay 2 needs 8 bytes = 2 bundles)
        assert_eq!(
            p1_outputs.len(),
            1,
            "P1 should get 1 output (after 2 bundles)"
        );

        // Verify P1's output (after Bundle 2, contains both bundles)
        match &p1_outputs[0].response {
            ServerResponse::GameData(data) => {
                assert_eq!(
                    data,
                    &vec![0x01, 0x00, 0xAA, 0xBB, 0x02, 0x00, 0xCC, 0xDD],
                    "P1's output should contain both bundles"
                );
            }
            _ => panic!("P1's output should be GameData"),
        }
    }

    #[test]
    fn test_cache_mechanism() {
        let mut manager = CachedGameSync::new(vec![1, 1]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();

        assert_eq!(outputs.0.len(), 2);
        assert!(matches!(outputs.0[0].response, ServerResponse::GameData(_)));

        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();

        let has_cache = outputs
            .0
            .iter()
            .any(|o| matches!(o.response, ServerResponse::GameCache(_)));
        assert!(has_cache);
    }

    #[test]
    fn test_three_players() {
        let mut manager = CachedGameSync::new(vec![1, 1, 2]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x00]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x02, 0x00]))
            .unwrap();

        assert_eq!(outputs.0.len(), 0); // P2 hasn't sent input yet

        // P2 sends input
        let outputs = manager
            .process_client_input(2, ClientInput::GameData(vec![0x03, 0x00, 0x04, 0x00]))
            .unwrap();

        assert!(outputs.0.len() >= 2);
        let p0_outputs: Vec<_> = outputs.0.iter().filter(|o| o.player_id == 0).collect();
        let p1_outputs: Vec<_> = outputs.0.iter().filter(|o| o.player_id == 1).collect();

        assert_eq!(p0_outputs.len(), 1);
        assert_eq!(p1_outputs.len(), 1);
    }

    #[test]
    fn test_gd_gc_pattern_delay_1() {
        let mut manager = CachedGameSync::new(vec![1, 1]);

        // Frame 1
        manager
            .process_client_input(0, ClientInput::GameData(vec![0xAA, 0xBB]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0xCC, 0xDD]))
            .unwrap();
        assert_eq!(outputs.0.len(), 2);

        // Frame 2
        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();
        assert!(matches!(
            outputs.0[0].response,
            ServerResponse::GameCache(_)
        ));

        // Frame 3
        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();
        assert!(matches!(
            outputs.0[0].response,
            ServerResponse::GameCache(_)
        ));

        // Frame 4
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x11, 0x22]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x33, 0x44]))
            .unwrap();
        assert!(matches!(outputs.0[0].response, ServerResponse::GameData(_)));
    }

    #[test]
    fn test_gd_gc_pattern_delay_2() {
        let mut manager = CachedGameSync::new(vec![2, 2]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0xAA, 0xBB, 0xAA, 0xBB]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0xCC, 0xDD, 0xCC, 0xDD]))
            .unwrap();
        assert_eq!(outputs.0.len(), 2);

        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();
        assert!(matches!(
            outputs.0[0].response,
            ServerResponse::GameCache(_)
        ));
    }

    #[test]
    fn test_gd_gc_creates_new_combined() {
        let mut manager = CachedGameSync::new(vec![1, 1]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        manager
            .process_client_input(1, ClientInput::GameData(vec![0x03, 0x04]))
            .unwrap();

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x05, 0x06]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();

        match &outputs.0[0].response {
            ServerResponse::GameData(data) => {
                assert_eq!(data, &vec![0x05, 0x06, 0x03, 0x04]);
            }
            _ => panic!("Should be GameData"),
        }
    }

    #[test]
    fn test_gc_gc_creates_new_combined() {
        let mut manager = CachedGameSync::new(vec![1, 1]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        manager
            .process_client_input(1, ClientInput::GameData(vec![0x03, 0x04]))
            .unwrap();

        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        manager
            .process_client_input(1, ClientInput::GameData(vec![0x05, 0x06]))
            .unwrap();

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x07, 0x08]))
            .unwrap();
        manager
            .process_client_input(1, ClientInput::GameCache(0))
            .unwrap();

        manager
            .process_client_input(0, ClientInput::GameCache(0))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(1))
            .unwrap();
        assert!(matches!(
            outputs.0[0].response,
            ServerResponse::GameCache(_)
        ));

        manager
            .process_client_input(0, ClientInput::GameCache(1))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameCache(1))
            .unwrap();
        match &outputs.0[0].response {
            ServerResponse::GameData(_) => {}
            _ => panic!("Should be GameData (new combination)"),
        }
    }

    #[test]
    fn test_delay_2_with_4_bytes_succeeds() {
        let mut manager = CachedGameSync::new(vec![2, 2]);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0xAA, 0xBB, 0xCC, 0xDD]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x11, 0x22, 0x33, 0x44]))
            .unwrap();

        assert_eq!(outputs.0.len(), 2);
    }

    #[test]
    fn test_invalid_player_id() {
        let mut manager = CachedGameSync::new(vec![1, 1]);
        let result = manager.process_client_input(2, ClientInput::GameData(vec![0x01, 0x02]));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GameSyncError::InvalidPlayerId { .. }
        ));
    }

    #[test]
    fn test_cache_position_not_found() {
        let mut manager = CachedGameSync::new(vec![1, 1]);
        let result = manager.process_client_input(0, ClientInput::GameCache(0));

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GameSyncError::CachePositionNotFound { .. }
        ));
    }

    #[test]
    fn test_player_delay() {
        let manager = CachedGameSync::new(vec![1, 2, 3]);
        assert_eq!(manager.get_player_delay(0), 1);
        assert_eq!(manager.get_player_delay(1), 2);
        assert_eq!(manager.get_player_delay(2), 3);
    }

    #[test]
    fn test_player_count() {
        let manager = CachedGameSync::new(vec![1, 1, 1]);
        assert_eq!(manager.player_count(), 3);
    }

    #[test]
    fn test_game_data_size() {
        let mut manager = CachedGameSync::new(vec![1, 1]);
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        assert_eq!(manager.sync.game_data_size, 2);
    }

    #[test]
    fn test_game_data_size_with_different_delays() {
        let mut manager = CachedGameSync::new(vec![1, 2]);
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        let result = manager
            .process_client_input(1, ClientInput::GameData(vec![0x03, 0x04, 0x05, 0x06]))
            .unwrap();
        assert_eq!(
            result.0,
            vec![CachedPlayerOutput {
                player_id: 0,
                response: ServerResponse::GameData(vec![0x01, 0x02, 0x03, 0x04])
            }]
        );
        let result = manager
            .process_client_input(0, ClientInput::GameData(vec![0x07, 0x08]))
            .unwrap();
        assert_eq!(
            result.0,
            vec![
                CachedPlayerOutput {
                    player_id: 0,
                    response: ServerResponse::GameData(vec![0x07, 0x08, 0x05, 0x06])
                },
                CachedPlayerOutput {
                    player_id: 1,
                    response: ServerResponse::GameData(vec![
                        0x01, 0x02, 0x03, 0x04, 0x07, 0x08, 0x05, 0x06
                    ])
                },
            ]
        );
    }
    #[test]
    fn test_game_data_size_drop() {
        let mut manager = CachedGameSync::new(vec![1, 1]);
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x01, 0x02]))
            .unwrap();
        manager.mark_player_dropped(0).unwrap();
        let result = manager
            .process_client_input(1, ClientInput::GameData(vec![0x03, 0x04]))
            .unwrap();
        assert_eq!(
            result.0,
            vec![
                CachedPlayerOutput {
                    player_id: 0,
                    response: ServerResponse::GameData(vec![0x01, 0x02, 0x03, 0x04])
                },
                CachedPlayerOutput {
                    player_id: 1,
                    response: ServerResponse::GameData(vec![0x01, 0x02, 0x03, 0x04])
                },
            ]
        );
        let result = manager
            .process_client_input(1, ClientInput::GameData(vec![0x05, 0x06]))
            .unwrap();
        assert_eq!(
            result.0,
            vec![
                CachedPlayerOutput {
                    player_id: 0,
                    response: ServerResponse::GameData(vec![0, 0, 5, 6]),
                },
                CachedPlayerOutput {
                    player_id: 1,
                    response: ServerResponse::GameData(vec![0, 0, 5, 6])
                },
            ]
        )
    }
}
