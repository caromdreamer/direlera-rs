// Simple Game Sync - Cleaner implementation with per-player send buffers
// Each player has their own independent send buffer

#![allow(dead_code)]

// logic description

use std::collections::{HashMap, VecDeque};

use tracing::warn;

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

/// FIFO cache with 256 slots. Positions are logical (0 = oldest, size-1 = newest),
/// matching the kaillera protocol spec. Lookup is O(1) via a content→indices map;
/// this yields the same positions a linear scan would, just faster.
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
    /// Logical position: 0 = oldest entry, size-1 = newest entry.
    pub fn find(&self, data: &[u8]) -> Option<u8> {
        let indices = self.index_map.get(data)?;
        let &abs_last = indices.back()?;
        let head = self.abs_tail - self.slots.len();
        // index_map only ever holds live entries: eviction pops the evicted abs index
        // in push(), so any abs found here is always within [head, abs_tail).
        debug_assert!(
            abs_last >= head,
            "index_map holds stale entry: abs_last={abs_last}, head={head}"
        );
        Some((abs_last - head) as u8)
    }

    /// Add data to cache (evicts oldest slot when full). Returns true if an eviction occurred.
    pub fn push(&mut self, data: Vec<u8>) -> bool {
        let evicted = if self.slots.len() >= 256 {
            let old = self.slots.pop_front().unwrap();
            if let Some(indices) = self.index_map.get_mut(&old) {
                indices.pop_front();
                if indices.is_empty() {
                    self.index_map.remove(&old);
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

    /// Get data at logical position (0 = oldest, size-1 = newest).
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

    /// Drain ready inputs (all players have input or are dropped) and process them
    /// This can be called without new input to check if drop events allow sending data
    pub fn drain_ready_inputs(&mut self) -> Result<Vec<PlayerOutput>, GameSyncError> {
        let mut results = Vec::new();

        // 모든 플레이어의 입력이 있는지 확인 (드롭된 플레이어 제외)
        while {
            let all_ready = self
                .player_input
                .iter()
                .enumerate()
                .all(|(i, buffer)| !buffer.is_empty() || self.dropped_players[i]);
            let has_any_input = self.player_input.iter().any(|buffer| !buffer.is_empty());
            all_ready && has_any_input
        } {
            // 각 플레이어로부터 하나씩 입력 추출
            // drop된 플레이어는 0으로 채운 데이터 생성
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

            // 모든 sender_buffer에 추출된 입력들을 추가
            for buffer in &mut self.sender_buffer {
                buffer.extend(extract_inputs.clone());
            }

            // 각 플레이어의 sender_buffer 확인 후 전송
            let players = self.player_delays.len();
            for (pid, buffer) in self.sender_buffer.iter_mut().enumerate() {
                // 필요한 개수 = delay * players (OneInput 개수)
                let require_len = self.player_delays[pid] * players;

                while buffer.len() >= require_len {
                    // OneInput들을 drain해서 flatten
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
        tracing::debug!(player_id, "marking player as dropped");
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
    /// When true, server→client outputs are always sent as full GameData and
    /// never as a GameCache index. Trades bandwidth for compatibility with
    /// diverse client cache implementations; latency is unaffected. In this mode
    /// the output cache is bypassed entirely (never queried or filled).
    disable_output_cache: bool,
}

impl CachedGameSync {
    /// Create a new cached game sync manager
    pub fn new(player_delays: Vec<usize>) -> Self {
        let player_count = player_delays.len();
        Self {
            sync: SimplestGameSync::new(player_delays.clone()),
            input_caches: (0..player_count).map(|_| InputCache::new()).collect(),
            output_caches: (0..player_count).map(|_| InputCache::new()).collect(),
            disable_output_cache: false,
        }
    }

    /// Force full-GameData downstream (never emit a GameCache index). See the
    /// `disable_output_cache` config option.
    pub fn with_output_cache_disabled(mut self, disabled: bool) -> Self {
        self.disable_output_cache = disabled;
        self
    }

    fn make_cached_output(
        &mut self,
        player_id: usize,
        output: Vec<u8>,
    ) -> (ServerResponse, bool, Option<usize>) {
        if self.disable_output_cache {
            (ServerResponse::GameData(output), false, None)
        } else if let Some(cache_pos) = self.output_caches[player_id].find(&output) {
            (ServerResponse::GameCache(cache_pos), false, None)
        } else {
            let before = self.output_caches[player_id].len();
            let overflowed = self.output_caches[player_id].push(output.clone());
            let after = self.output_caches[player_id].len();
            let milestone = (before / 64 != after / 64).then_some(after);
            (ServerResponse::GameData(output), overflowed, milestone)
        }
    }

    /// Process client input with cache support.
    /// Returns (outputs, cache_overflowed, milestone) where:
    /// - cache_overflowed: true if any output cache eviction occurred (256+ reached)
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
                // Store in client's input cache
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
            let (cached_output, overflowed, crossed) =
                self.make_cached_output(raw_output.player_id, raw_output.output);
            if overflowed {
                cache_overflowed = true;
            }
            if crossed.is_some() {
                milestone = crossed;
            }

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
            let (cached_output, _, _) =
                self.make_cached_output(raw_output.player_id, raw_output.output);

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

/// L1: per-player startup delay buffer placed in FRONT of the combiner.
///
/// Pipeline order (the GameCache hazard below is why this ordering matters):
///   input-cache resolve (here) -> startup FIFO (here) -> combine + output-cache (`inner`)
///
/// During startup, inputs are saved and zero frames are returned. Once warmup
/// ends, saved startup inputs are flushed one per new packet while the current
/// packet is discarded, matching EmuLinker's catch-up behavior. After the saved
/// queue drains, current inputs are combined immediately.
///
/// GameCache references are resolved to bytes HERE, up front, and only resolved
/// bytes are stored in the FIFO. Resolving a *delayed* GameCache reference later (at
/// drain time) would read a client cache whose contents have since shifted -> silent
/// desync. So resolution must precede the delay.
///
/// A player's startup delay of 0 is a transparent passthrough.
#[derive(Debug, Clone)]
pub struct DelayedGameSync {
    inner: CachedGameSync,
    /// Per-player client-input cache, for resolving incoming GameCache references.
    input_caches: Vec<InputCache>,
    /// Per-player startup FIFO of resolved input bytes awaiting release.
    queues: Vec<VecDeque<Vec<u8>>>,
    /// Per-player count of warmup zero-frames already emitted.
    warmup_sent: Vec<usize>,
    /// Per-player startup zero-frame count.
    total_delays: Vec<usize>,
    /// One player's per-frame input size, learned from the first input (zero sizing).
    /// Assumes conn=1 (one frame per packet); revisit for batched conn types.
    frame_size: usize,
    player_count: usize,
}

impl DelayedGameSync {
    /// Wrap a combiner with a startup warmup buffer.
    pub fn new(inner: CachedGameSync, total_delay: usize) -> Self {
        let player_count = inner.player_count();
        Self::with_player_delays(inner, vec![total_delay; player_count])
    }

    /// Wrap a combiner with per-player startup warmup buffers.
    pub fn with_player_delays(inner: CachedGameSync, total_delays: Vec<usize>) -> Self {
        let player_count = inner.player_count();
        assert_eq!(
            total_delays.len(),
            player_count,
            "delay count must match player count"
        );
        Self {
            input_caches: (0..player_count).map(|_| InputCache::new()).collect(),
            queues: (0..player_count).map(|_| VecDeque::new()).collect(),
            warmup_sent: vec![0; player_count],
            total_delays,
            frame_size: 0,
            player_count,
            inner,
        }
    }

    pub fn process_client_input(
        &mut self,
        player_id: usize,
        input: ClientInput,
    ) -> Result<(Vec<CachedPlayerOutput>, bool, Option<usize>), GameSyncError> {
        if player_id >= self.player_count {
            return Err(GameSyncError::InvalidPlayerId {
                player_id,
                player_count: self.player_count,
            });
        }

        // L0: resolve to raw bytes (and mirror the client's input cache so future
        // GameCache references from this client resolve correctly).
        let bytes = match input {
            ClientInput::GameData(d) => {
                self.input_caches[player_id].push(d.clone());
                d
            }
            ClientInput::GameCache(pos) => self.input_caches[player_id]
                .get(pos)
                .ok_or(GameSyncError::CachePositionNotFound {
                    player_id,
                    position: pos,
                })?
                .to_vec(),
        };

        if self.frame_size == 0 && !bytes.is_empty() {
            self.frame_size = bytes.len();
        }

        // Startup warmup: save real inputs, return zero frames.
        let total_delay = self.total_delays[player_id];
        if self.warmup_sent[player_id] < total_delay {
            self.queues[player_id].push_back(bytes);
            self.warmup_sent[player_id] += 1;
            let zero = vec![0u8; self.player_count * self.frame_size];
            let (response, _, _) = self.inner.make_cached_output(player_id, zero);
            return Ok((
                vec![CachedPlayerOutput {
                    player_id,
                    response,
                }],
                false,
                None,
            ));
        }

        // Catch-up: while startup inputs remain, flush them and drop the current
        // packet. EmuLinker does this, so lag returns to normal after startup.
        if let Some(due) = self.queues[player_id].pop_front() {
            return self
                .inner
                .process_client_input(player_id, ClientInput::GameData(due));
        }

        self.inner
            .process_client_input(player_id, ClientInput::GameData(bytes))
    }

    pub fn mark_player_dropped(
        &mut self,
        player_id: usize,
    ) -> Result<Vec<CachedPlayerOutput>, GameSyncError> {
        // A dropped player's buffered (delayed) inputs are moot; discard them.
        if player_id < self.player_count {
            self.queues[player_id].clear();
        }
        self.inner.mark_player_dropped(player_id)
    }

    pub fn is_player_dropped(&self, player_id: usize) -> bool {
        self.inner.is_player_dropped(player_id)
    }

    pub fn player_count(&self) -> usize {
        self.player_count
    }

    pub fn get_player_delay(&self, player_id: usize) -> usize {
        self.inner.get_player_delay(player_id)
    }

    pub fn all_players_dropped(&self) -> bool {
        self.inner.sync.all_players_dropped()
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
    fn test_output_cache_disabled_always_game_data() {
        // Same scenario as test_cache_mechanism, but with output caching disabled:
        // the repeated frame must still be sent as full GameData, never a
        // GameCache index reference.
        let mut manager = CachedGameSync::new(vec![1, 1]).with_output_cache_disabled(true);

        manager
            .process_client_input(0, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();
        assert!(outputs
            .0
            .iter()
            .all(|o| matches!(o.response, ServerResponse::GameData(_))));

        // Repeat the identical frame — would normally resolve to a GameCache hit.
        manager
            .process_client_input(0, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();
        let outputs = manager
            .process_client_input(1, ClientInput::GameData(vec![0x00, 0x00]))
            .unwrap();
        assert!(
            outputs
                .0
                .iter()
                .all(|o| matches!(o.response, ServerResponse::GameData(_))),
            "output cache disabled must never emit GameCache"
        );
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

    /// Verify logical-index semantics survive cache eviction (256+ entries).
    /// Before the fix, find() returned abs%256 so position 0 pointed to the newest
    /// entry after the first wrap-around instead of the oldest — causing wrong key replay.
    #[test]
    fn test_input_cache_logical_positions_after_eviction() {
        let mut cache = InputCache::new();

        // Fill cache with 256 unique entries.
        for i in 0u8..=255 {
            cache.push(vec![i, 0]);
        }
        assert_eq!(cache.len(), 256);
        // Position 0 = oldest = [0, 0], position 255 = newest = [255, 0].
        assert_eq!(cache.find(&[0, 0]), Some(0));
        assert_eq!(cache.find(&[255, 0]), Some(255));
        assert_eq!(cache.get(0), Some([0u8, 0].as_ref()));
        assert_eq!(cache.get(255), Some([255u8, 0].as_ref()));

        // Push one more entry — evicts [0, 0].  New entry [100, 1] lands at pos 255.
        cache.push(vec![100, 1]);
        assert_eq!(cache.len(), 256);
        assert_eq!(
            cache.find(&[0, 0]),
            None,
            "[0,0] was evicted, must not be found"
        );
        assert_eq!(
            cache.find(&[100, 1]),
            Some(255),
            "newest entry must be at logical pos 255"
        );
        assert_eq!(
            cache.find(&[1, 0]),
            Some(0),
            "oldest surviving entry must be at logical pos 0"
        );
        assert_eq!(cache.get(255), Some([100u8, 1].as_ref()));
        assert_eq!(cache.get(0), Some([1u8, 0].as_ref()));
    }

    /// Verify output cache positions sent to clients remain consistent after eviction.
    #[test]
    fn test_output_cache_position_after_eviction() {
        let mut cache = InputCache::new();
        // Push 257 unique entries (i as two bytes) so one eviction occurs.
        // i=0 → [0,0], i=1 → [0,1], ..., i=255 → [0,255], i=256 → [1,0]
        for i in 0u16..257 {
            cache.push(vec![(i >> 8) as u8, (i & 0xFF) as u8]);
        }
        // [0,0] (abs 0) was evicted; [0,1] (abs 1) is oldest at pos 0; [1,0] (abs 256) is newest at pos 255.
        assert_eq!(cache.find(&[0, 0]), None, "[0,0] was evicted");
        assert_eq!(cache.find(&[0, 1]), Some(0), "oldest surviving = pos 0");
        assert_eq!(cache.find(&[0, 255]), Some(254), "abs 255 = pos 254");
        assert_eq!(cache.find(&[1, 0]), Some(255), "newest entry = pos 255");
        assert_eq!(cache.get(0), Some([0u8, 1].as_ref()));
        assert_eq!(cache.get(255), Some([1u8, 0].as_ref()));
    }

    // --- DelayedGameSync (warmup + fixed delay) ---

    fn gd(bytes: Vec<u8>) -> ClientInput {
        ClientInput::GameData(bytes)
    }

    fn decode_client_output(cache: &mut Vec<Vec<u8>>, response: &ServerResponse) -> Vec<u8> {
        match response {
            ServerResponse::GameData(data) => {
                if cache.len() >= 256 {
                    cache.remove(0);
                }
                cache.push(data.clone());
                data.clone()
            }
            ServerResponse::GameCache(pos) => cache[*pos as usize].clone(),
        }
    }

    #[test]
    fn test_delay_passthrough_d0() {
        // D=0 must be a transparent passthrough: input is combined immediately,
        // no warmup zeros (so the layer can be inserted before RTT-based D is wired).
        let inner = CachedGameSync::new(vec![1]).with_output_cache_disabled(true);
        let mut d = DelayedGameSync::new(inner, 0);
        let (out, _, _) = d.process_client_input(0, gd(vec![1, 2])).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].response, ServerResponse::GameData(vec![1, 2]));
    }

    #[test]
    fn test_warmup_then_delay_single_player() {
        // 1 player, D=2: first two inputs return zero frames (warmup), then output
        // is the real input delayed by exactly 2 frames.
        let inner = CachedGameSync::new(vec![1]).with_output_cache_disabled(true);
        let mut d = DelayedGameSync::new(inner, 2);

        let (o1, _, _) = d.process_client_input(0, gd(vec![1, 2])).unwrap();
        assert_eq!(
            o1[0].response,
            ServerResponse::GameData(vec![0, 0]),
            "warmup #1 = zero"
        );
        let (o2, _, _) = d.process_client_input(0, gd(vec![3, 4])).unwrap();
        assert_eq!(
            o2[0].response,
            ServerResponse::GameData(vec![0, 0]),
            "warmup #2 = zero"
        );

        let (o3, _, _) = d.process_client_input(0, gd(vec![5, 6])).unwrap();
        assert_eq!(
            o3[0].response,
            ServerResponse::GameData(vec![1, 2]),
            "delayed by 2"
        );
        let (o4, _, _) = d.process_client_input(0, gd(vec![7, 8])).unwrap();
        assert_eq!(
            o4[0].response,
            ServerResponse::GameData(vec![3, 4]),
            "delayed by 2"
        );
        let (o5, _, _) = d.process_client_input(0, gd(vec![9, 10])).unwrap();
        assert_eq!(
            o5[0].response,
            ServerResponse::GameData(vec![9, 10]),
            "caught up after startup inputs drain"
        );
    }

    #[test]
    fn test_warmup_outputs_stay_in_output_cache() {
        let mut d = DelayedGameSync::new(CachedGameSync::new(vec![1]), 6);
        let inputs = vec![
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x01, 0x00],
            vec![0x01, 0x00],
            vec![0x01, 0x00],
            vec![0x02, 0x00],
            vec![0x02, 0x00],
            vec![0x04, 0x00],
            vec![0x04, 0x00],
            vec![0x04, 0x00],
            vec![0x08, 0x00],
            vec![0x08, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x10, 0x00],
            vec![0x10, 0x00],
        ];
        let expected = vec![
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x01, 0x00],
            vec![0x01, 0x00],
            vec![0x01, 0x00],
            vec![0x02, 0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x00],
            vec![0x10, 0x00],
            vec![0x10, 0x00],
        ];

        let mut client_cache = Vec::new();
        let mut decoded = Vec::new();
        for input in inputs {
            let (outputs, _, _) = d.process_client_input(0, gd(input)).unwrap();
            assert_eq!(outputs.len(), 1);
            decoded.push(decode_client_output(
                &mut client_cache,
                &outputs[0].response,
            ));
        }

        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_warmup_two_players_then_combine() {
        // 2 players, D=1. Warmup emits a zero combined-frame (size = players *
        // frame_size = 4) to each, then the combiner runs on the delayed pair.
        let inner = CachedGameSync::new(vec![1, 1]).with_output_cache_disabled(true);
        let mut d = DelayedGameSync::new(inner, 1);

        let (a, _, _) = d.process_client_input(0, gd(vec![1, 2])).unwrap();
        assert_eq!(a[0].response, ServerResponse::GameData(vec![0, 0, 0, 0]));
        let (b, _, _) = d.process_client_input(1, gd(vec![3, 4])).unwrap();
        assert_eq!(b[0].response, ServerResponse::GameData(vec![0, 0, 0, 0]));

        // p0 releases its delayed [1,2]; p1 hasn't released yet -> nothing combined.
        let (c, _, _) = d.process_client_input(0, gd(vec![5, 6])).unwrap();
        assert!(c.is_empty(), "combine waits for all players");

        // p1 releases [3,4] -> combine of the delayed pair [1,2]+[3,4] to both.
        let (e, _, _) = d.process_client_input(1, gd(vec![7, 8])).unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].response, ServerResponse::GameData(vec![1, 2, 3, 4]));
        assert_eq!(e[1].response, ServerResponse::GameData(vec![1, 2, 3, 4]));

        let (f, _, _) = d.process_client_input(0, gd(vec![9, 10])).unwrap();
        assert!(f.is_empty(), "combine waits for all players");
        let (g, _, _) = d.process_client_input(1, gd(vec![11, 12])).unwrap();
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].response, ServerResponse::GameData(vec![9, 10, 11, 12]));
        assert_eq!(g[1].response, ServerResponse::GameData(vec![9, 10, 11, 12]));
    }

    #[test]
    fn test_per_player_warmup_delays() {
        let inner = CachedGameSync::new(vec![1, 1]).with_output_cache_disabled(true);
        let mut d = DelayedGameSync::with_player_delays(inner, vec![2, 1]);

        let (p0_a, _, _) = d.process_client_input(0, gd(vec![1, 0])).unwrap();
        assert_eq!(p0_a[0].response, ServerResponse::GameData(vec![0, 0, 0, 0]));
        let (p1_a, _, _) = d.process_client_input(1, gd(vec![10, 0])).unwrap();
        assert_eq!(p1_a[0].response, ServerResponse::GameData(vec![0, 0, 0, 0]));

        let (p1_b, _, _) = d.process_client_input(1, gd(vec![11, 0])).unwrap();
        assert!(p1_b.is_empty(), "p1 released, but p0 has not yet released");
        let (p0_b, _, _) = d.process_client_input(0, gd(vec![2, 0])).unwrap();
        assert_eq!(p0_b[0].response, ServerResponse::GameData(vec![0, 0, 0, 0]));

        let (p0_c, _, _) = d.process_client_input(0, gd(vec![3, 0])).unwrap();
        assert_eq!(p0_c.len(), 2);
        assert_eq!(
            p0_c[0].response,
            ServerResponse::GameData(vec![1, 0, 10, 0])
        );
        assert_eq!(
            p0_c[1].response,
            ServerResponse::GameData(vec![1, 0, 10, 0])
        );

        let (p0_d, _, _) = d.process_client_input(0, gd(vec![4, 0])).unwrap();
        assert!(p0_d.is_empty(), "p0 released, but p1 has not sent next");
        let (p1_c, _, _) = d.process_client_input(1, gd(vec![12, 0])).unwrap();
        assert_eq!(p1_c.len(), 2);
        assert_eq!(
            p1_c[0].response,
            ServerResponse::GameData(vec![2, 0, 12, 0])
        );
        assert_eq!(
            p1_c[1].response,
            ServerResponse::GameData(vec![2, 0, 12, 0])
        );
    }
}
