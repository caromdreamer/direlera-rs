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

    // Server configuration
    pub config: Arc<crate::Config>,
}

impl AppState {
    pub fn new(tx: mpsc::Sender<crate::Message>, config: crate::Config) -> Self {
        Self {
            clients_by_addr: Arc::new(RwLock::new(HashMap::new())),
            clients_by_id: Arc::new(RwLock::new(HashMap::new())),
            games: Arc::new(RwLock::new(HashMap::new())),
            next_game_id: Arc::new(AtomicU32::new(1)),
            next_user_id: Arc::new(AtomicU16::new(1)),
            tx,
            config: Arc::new(config),
        }
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

        let mut id_map = self.clients_by_id.write().await;
        id_map.insert(session_id, client);

        let mut addr_map = self.clients_by_addr.write().await;
        addr_map.insert(addr, session_id);
    }

    pub async fn remove_client(&self, addr: &SocketAddr) -> Option<ClientInfo> {
        let mut addr_map = self.clients_by_addr.write().await;
        let session_id = addr_map.remove(addr)?;

        let mut id_map = self.clients_by_id.write().await;
        id_map.remove(&session_id)
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

    pub async fn update_client<F, R, E>(&self, addr: &SocketAddr, f: F) -> Result<R, E>
    where
        F: FnOnce(&mut ClientInfo) -> Result<R, E>,
    {
        let addr_map = self.clients_by_addr.read().await;
        let session_id = addr_map
            .get(addr)
            .cloned()
            .ok_or_else(|| panic!("Client not found"))?;
        drop(addr_map);

        let mut id_map = self.clients_by_id.write().await;
        let client = id_map
            .get_mut(&session_id)
            .ok_or_else(|| panic!("Client not found"))?;

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
    pub ping: u32, // Average ping value (average of last 5 measurements, excluding first)
    pub player_status: PlayerStatus,
    pub game_id: Option<u32>,
    pub last_ping_time: Option<Instant>, // Timestamp when SERVER_TO_CLIENT_ACK was sent (for RTT measurement)
    pub ack_count: u16,
    pub ping_samples: Vec<u32>, // Recent RTT measurements for averaging (max 5, excluding first measurement)
    /// Unix timestamp (seconds) of the last received packet — updated lock-free
    pub last_activity_secs: Arc<AtomicU64>,
    /// Packet generator for this client (handles sequence numbers and redundancy)
    pub packet_generator: crate::kaillera::protocol::UDPPacketGenerator,
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

/// Player information stored in GameInfo (immutable after joining)
/// These fields don't change once a player joins the game
#[derive(Debug, Clone)]
pub struct GamePlayerInfo {
    pub addr: std::net::SocketAddr,
    pub username: Vec<u8>, // Store as bytes to preserve original encoding
    pub user_id: u16,
    pub conn_type: u8,
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
    pub sync_manager: Option<simplest_game_sync::CachedGameSync>,
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
