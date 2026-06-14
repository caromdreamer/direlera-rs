use crate::simplest_game_sync;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, Mutex as GameMutex, RwLock};
use uuid::Uuid;

type PlayerStatus = u8;
pub const PLAYER_STATUS_PLAYING: PlayerStatus = 0;
pub const PLAYER_STATUS_IDLE: PlayerStatus = 1;
pub const PLAYER_STATUS_NET_SYNC: PlayerStatus = 2;

// AppState - centralized state with RwLock for efficiency
#[derive(Debug)]
pub struct AppState {
    // RwLock: multiple readers, exclusive writer
    pub clients_by_addr: Arc<RwLock<HashMap<SocketAddr, Uuid>>>,
    pub clients_by_id: Arc<RwLock<HashMap<Uuid, ClientInfo>>>,
    pub games: Arc<RwLock<HashMap<u32, Arc<GameMutex<GameInfo>>>>>,

    // Atomic: lock-free counter increment
    pub next_game_id: Arc<AtomicU32>,
    pub next_user_id: Arc<AtomicU16>,

    pub tx: mpsc::Sender<crate::Message>,

    /// Requests immediate teardown of a client's UDP session in the
    /// SessionManager. Used on normal quit / stale eviction so the orphaned
    /// session task doesn't linger until SESSION_TIMEOUT.
    pub session_close_tx: mpsc::Sender<SocketAddr>,

    // Server configuration
    pub config: Arc<crate::Config>,

    pub start_time: std::time::Instant,
}

impl AppState {
    pub fn new(
        tx: mpsc::Sender<crate::Message>,
        session_close_tx: mpsc::Sender<SocketAddr>,
        config: crate::Config,
    ) -> Self {
        Self {
            clients_by_addr: Arc::new(RwLock::new(HashMap::new())),
            clients_by_id: Arc::new(RwLock::new(HashMap::new())),
            games: Arc::new(RwLock::new(HashMap::new())),
            next_game_id: Arc::new(AtomicU32::new(1)),
            next_user_id: Arc::new(AtomicU16::new(1)),
            tx,
            session_close_tx,
            config: Arc::new(config),
            start_time: std::time::Instant::now(),
        }
    }

    /// Ask the SessionManager to tear down this address's session now.
    /// Best-effort: a full channel or absent session is harmless (the session
    /// would still expire via SESSION_TIMEOUT).
    pub async fn close_session(&self, addr: &SocketAddr) {
        let _ = self.session_close_tx.send(*addr).await;
    }

    // Lock-free ID generation
    pub fn next_game_id(&self) -> u32 {
        self.next_game_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_user_id(&self) -> u16 {
        self.next_user_id.fetch_add(1, Ordering::SeqCst)
    }

    // Read - multiple threads can read simultaneously
    pub async fn get_client(&self, addr: &SocketAddr) -> Option<ClientInfo> {
        let addr_map = self.clients_by_addr.read().await;
        let session_id = addr_map.get(addr)?;
        let id_map = self.clients_by_id.read().await;
        id_map.get(session_id).cloned()
    }

    // Write - exclusive lock
    pub async fn add_client(&self, addr: SocketAddr, client: ClientInfo) {
        let session_id = client.session_id;

        // Keep the same lock order as update/remove paths (addr -> id). Login
        // bursts otherwise can deadlock against ACK handling, which looks like
        // clients timing out before receiving any server ACK.
        let mut addr_map = self.clients_by_addr.write().await;
        let mut id_map = self.clients_by_id.write().await;
        id_map.insert(session_id, client);
        addr_map.insert(addr, session_id);

        metrics::gauge!("active_sessions_total").increment(1.0);
    }

    pub async fn remove_client(&self, addr: &SocketAddr) -> Option<ClientInfo> {
        let mut addr_map = self.clients_by_addr.write().await;
        let session_id = addr_map.remove(addr)?;

        let mut id_map = self.clients_by_id.write().await;
        let client = id_map.remove(&session_id);
        if client.is_some() {
            metrics::gauge!("active_sessions_total").decrement(1.0);
        }
        client
    }

    /// Find and remove any existing client with the same username.
    /// Used to evict stale sessions when a user reconnects from a new address.
    pub async fn remove_client_by_username(
        &self,
        username: &[u8],
    ) -> Option<(SocketAddr, ClientInfo)> {
        let session_id = {
            let id_map = self.clients_by_id.read().await;
            id_map
                .iter()
                .find(|(_, c)| c.username == username)
                .map(|(id, _)| *id)?
        };

        let old_addr = {
            let mut addr_map = self.clients_by_addr.write().await;
            let addr = addr_map
                .iter()
                .find(|(_, v)| **v == session_id)
                .map(|(k, _)| *k)?;
            addr_map.remove(&addr);
            addr
        };

        let mut id_map = self.clients_by_id.write().await;
        let client = id_map.remove(&session_id)?;

        metrics::gauge!("active_sessions_total").decrement(1.0);
        Some((old_addr, client))
    }

    /// Read-only lookup of a client by username (does not remove).
    pub async fn find_client_by_username(
        &self,
        username: &[u8],
    ) -> Option<(SocketAddr, ClientInfo)> {
        let (session_id, client) = {
            let id_map = self.clients_by_id.read().await;
            id_map
                .iter()
                .find(|(_, c)| c.username == username)
                .map(|(id, c)| (*id, c.clone()))?
        };

        let addr_map = self.clients_by_addr.read().await;
        let addr = addr_map
            .iter()
            .find(|(_, &v)| v == session_id)
            .map(|(k, _)| *k)?;

        Some((addr, client))
    }

    /// Update the last-activity timestamp for a client (lock-free after addr lookup).
    pub async fn update_client_activity(&self, addr: &SocketAddr) {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let addr_map = self.clients_by_addr.read().await;
        if let Some(&session_id) = addr_map.get(addr) {
            let id_map = self.clients_by_id.read().await;
            if let Some(client) = id_map.get(&session_id) {
                client.last_activity_secs.store(secs, Ordering::Relaxed);
            }
        }
    }

    // Get all client addresses
    pub async fn get_all_client_addrs(&self) -> Vec<SocketAddr> {
        let addr_map = self.clients_by_addr.read().await;
        addr_map.keys().cloned().collect()
    }

    // Game operations
    pub async fn add_game(&self, game_id: u32, game: GameInfo) {
        let mut games = self.games.write().await;
        games.insert(game_id, Arc::new(GameMutex::new(game)));
        metrics::gauge!("active_games_total").increment(1.0);
    }

    pub async fn get_game(&self, game_id: u32) -> Option<GameInfo> {
        let arc = {
            let games = self.games.read().await;
            games.get(&game_id)?.clone()
        };
        let guard = arc.lock().await;
        Some(guard.clone())
    }

    pub async fn remove_game(&self, game_id: u32) -> Option<GameInfo> {
        let arc = {
            let mut games = self.games.write().await;
            games.remove(&game_id)?
        };
        metrics::gauge!("active_games_total").decrement(1.0);
        let guard = arc.lock().await;
        Some(guard.clone())
    }

    /// Update a specific game under its own per-game lock (not the global HashMap lock).
    /// Multiple games can be updated concurrently.
    pub async fn update_game<F, R, E>(&self, game_id: u32, f: F) -> Result<R, E>
    where
        F: FnOnce(&mut GameInfo) -> Result<R, E>,
    {
        let arc = {
            let games = self.games.read().await;
            games
                .get(&game_id)
                .unwrap_or_else(|| panic!("Game not found: {}", game_id))
                .clone()
        };
        let mut game = arc.lock().await;
        f(&mut game)
    }

    /// Get the per-game Arc<Mutex> for direct access patterns.
    pub async fn get_game_arc(&self, game_id: u32) -> Option<Arc<GameMutex<GameInfo>>> {
        let games = self.games.read().await;
        games.get(&game_id).cloned()
    }

    pub async fn update_client<F, R>(&self, addr: &SocketAddr, f: F) -> color_eyre::Result<R>
    where
        F: FnOnce(&mut ClientInfo) -> color_eyre::Result<R>,
    {
        use color_eyre::eyre::eyre;
        let addr_map = self.clients_by_addr.read().await;
        let session_id = addr_map
            .get(addr)
            .cloned()
            .ok_or_else(|| eyre!("Client not found in addr_map: {}", addr))?;
        drop(addr_map);

        let mut id_map = self.clients_by_id.write().await;
        let client = id_map
            .get_mut(&session_id)
            .ok_or_else(|| eyre!("Client not found in id_map: {}", addr))?;

        f(client)
    }
}

// ClientInfo and GameInfo structs need to be accessible in both files
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub session_id: Uuid,
    pub username: Vec<u8>, // Store as bytes to preserve original encoding (CP949, etc.)
    pub emulator_name: Vec<u8>, // Store as bytes to preserve original encoding
    pub conn_type: u8,
    pub user_id: u16,
    pub ping: u32, // Login-time ping (ms): mean RTT over the ACK handshake round trips
    pub player_status: PlayerStatus,
    pub game_id: Option<u32>,
    pub last_ping_time: Option<Instant>, // Timestamp when SERVER_TO_CLIENT_ACK was sent (for RTT measurement)
    pub ack_count: u16,
    pub ping_total: std::time::Duration, // Accumulated RTT across handshake round trips; divided once at the end (full precision, no per-sample ms truncation)
    /// Unix timestamp (seconds) of the last received packet — updated lock-free
    pub last_activity_secs: Arc<AtomicU64>,
    /// Packet generator for this client (handles sequence numbers and redundancy)
    pub packet_generator: crate::kaillera::protocol::UDPPacketGenerator,
    /// The long-lived per-session tracing span. Handlers record session context
    /// (ping, game_id, ...) onto this so it propagates to every child event.
    pub session_span: tracing::Span,
}

impl ClientInfo {
    #[allow(dead_code)]
    /// Get username as String (for logging/display, uses lossy conversion)
    pub fn username_str(&self) -> String {
        String::from_utf8_lossy(&self.username).to_string()
    }

    #[allow(dead_code)]
    /// Get emulator name as String (for logging/display, uses lossy conversion)
    pub fn emulator_name_str(&self) -> String {
        String::from_utf8_lossy(&self.emulator_name).to_string()
    }

    #[allow(dead_code)]
    /// Get username for logging (safe display - shows ASCII and hex for non-ASCII)
    pub fn username_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.username)
    }

    #[allow(dead_code)]
    /// Get emulator name for logging (safe display)
    pub fn emulator_name_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.emulator_name)
    }
}

pub const GAME_STATUS_WAITING: u8 = 0;
pub const GAME_STATUS_PLAYING: u8 = 1;
#[allow(dead_code)]
pub const GAME_STATUS_NET_SYNC: u8 = 2;

/// Player information stored in GameInfo.
/// The Vec position of a player is also its lockstep sync-slot index, which must
/// not move while a game is live — see `left_room`.
#[derive(Debug, Clone)]
pub struct GamePlayerInfo {
    pub addr: std::net::SocketAddr,
    pub username: Vec<u8>, // Store as bytes to preserve original encoding
    pub user_id: u16,
    pub conn_type: u8,
    /// Silence grace before stall-resend kicks in. Sized from the START_GAME send
    /// window so normal lockstep backpressure is not treated as a stall.
    pub input_stall_grace: Option<std::time::Duration>,
    /// Timestamp of last received input packet (game_data or cache), for pacing.
    pub last_game_data_recv: Option<std::time::Instant>,
    /// EWMA baseline of the inter-arrival interval (seconds). Self-normalizes the
    /// pace per game (fps / conn_type / batching independent) so we can report the
    /// current/baseline ratio as a smoothness signal.
    pub interval_baseline_secs: Option<f64>,
    /// True once the player left the room (QUIT / reap) while a game was still
    /// live. The Vec index is the sync slot and can't move mid-game, so we
    /// tombstone instead of removing; the entry is purged when the game ends.
    pub left_room: bool,
}

impl GamePlayerInfo {
    #[allow(dead_code)]
    /// Get username as String (for logging/display, uses lossy conversion)
    pub fn username_str(&self) -> String {
        String::from_utf8_lossy(&self.username).to_string()
    }

    /// Get username for logging (safe display)
    #[allow(dead_code)]
    pub fn username_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.username)
    }
}

/// Precomputed Prometheus label values for a game. These never change after
/// creation, and the metrics hot path runs per input packet, so we build them
/// once and share via Arc (GameInfo is cloned per packet — keep that cheap).
/// `game_uid` is a per-game UUID used only for observability; the wire-protocol
/// `game_id` (short, sent to clients) is unchanged. The UUID makes each game a
/// distinct metric series even across server restarts (sequential game_id resets
/// to 1 on restart and would otherwise merge with the previous run's series).
#[derive(Debug, Clone)]
pub struct GameMetricLabels {
    pub game_uid: String,
    pub game_name: String,
    pub emulator_name: String,
}

/// Pre-registered metric handles for a playing game. Holding handles avoids
/// rebuilding label vectors and looking up metric keys on every input packet.
#[derive(Debug, Clone)]
pub struct GameMetricHandles {
    pub input_interval: metrics::Histogram,
    pub input_pace_ratio: metrics::Histogram,
    pub game_data_processing: metrics::Histogram,
    pub game_cache_processing: metrics::Histogram,
}

#[derive(Debug, Clone)]
pub struct GameInfo {
    pub game_id: u32,
    pub game_name: Vec<u8>,     // Store as bytes to preserve original encoding
    pub emulator_name: Vec<u8>, // Store as bytes to preserve original encoding
    pub owner: Vec<u8>,         // Store as bytes to preserve original encoding
    pub owner_user_id: u16,     // Owner's user_id for authorization checks
    pub num_players: u8,
    pub max_players: u8,
    pub game_status: u8, // 0=Waiting, 1=Playing, 2=Netsync
    // Player information in order (indexed by player_id)
    pub players: Vec<GamePlayerInfo>,
    // New: SimpleGameSync for frame synchronization
    pub sync_manager: Option<simplest_game_sync::DelayedGameSync>,
    /// Precomputed metric labels (uid/title/emulator), shared cheaply on clone.
    pub metric_labels: std::sync::Arc<GameMetricLabels>,
    /// Metric handles installed when the game starts and player count is fixed.
    pub metric_handles: Option<std::sync::Arc<GameMetricHandles>>,
}

impl GameInfo {
    #[allow(dead_code)]
    /// Get game name as String (for logging/display, uses lossy conversion)
    pub fn game_name_str(&self) -> String {
        String::from_utf8_lossy(&self.game_name).to_string()
    }

    /// Get emulator name as String (for logging/display, uses lossy conversion)
    #[allow(dead_code)]
    pub fn emulator_name_str(&self) -> String {
        String::from_utf8_lossy(&self.emulator_name).to_string()
    }

    /// Get owner name as String (for logging/display, uses lossy conversion)
    #[allow(dead_code)]
    pub fn owner_str(&self) -> String {
        String::from_utf8_lossy(&self.owner).to_string()
    }

    /// Get game name for logging (safe display)
    #[allow(dead_code)]
    pub fn game_name_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.game_name)
    }

    /// Get emulator name for logging (safe display)
    #[allow(dead_code)]
    pub fn emulator_name_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.emulator_name)
    }

    /// Get owner name for logging (safe display)
    #[allow(dead_code)]
    pub fn owner_for_log(&self) -> String {
        crate::handlers::util::bytes_for_log(&self.owner)
    }
}
