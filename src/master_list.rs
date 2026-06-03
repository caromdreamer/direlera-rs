use crate::state::{AppState, GAME_STATUS_WAITING};
use crate::{MasterEndpoint, MasterListConfig, MasterProtocol};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

const VERSION: &str = concat!("direlera/", env!("CARGO_PKG_VERSION"));
const INITIAL_DELAY: Duration = Duration::from_secs(10);
const TOUCH_INTERVAL: Duration = Duration::from_secs(60);

// Tried in order until one responds with a valid IP.
const PUBLIC_IP_SERVICES: &[&str] = &[
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
];

pub async fn run(state: Arc<AppState>) {
    if !state.config.master_list.enabled {
        info!("Master list reporting disabled");
        return;
    }

    let Ok(client) = Client::builder().timeout(Duration::from_secs(5)).build() else {
        warn!("Failed to build HTTP client for master list reporting");
        return;
    };

    let server_address = if state.config.master_list.server_address.is_empty() {
        match detect_public_ip(&client).await {
            Some(ip) => {
                info!(ip, "Auto-detected public IP for master list reporting");
                ip
            }
            None => {
                warn!("Could not detect public IP — skipping master list reporting");
                return;
            }
        }
    } else {
        state.config.master_list.server_address.clone()
    };

    info!(
        server_name = state.config.master_list.server_name.as_str(),
        server_address = server_address.as_str(),
        server_count = state.config.master_list.servers.len(),
        "Master list reporting enabled"
    );

    tokio::time::sleep(INITIAL_DELAY).await;

    loop {
        report(&client, &state, &server_address).await;
        tokio::time::sleep(TOUCH_INTERVAL).await;
    }
}

async fn detect_public_ip(client: &Client) -> Option<String> {
    for &service in PUBLIC_IP_SERVICES {
        match client.get(service).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.text().await {
                    let ip = body.trim().to_string();
                    if !ip.is_empty() {
                        return Some(ip);
                    }
                }
            }
            Ok(_) => debug!(service, "Public IP service returned non-200"),
            Err(e) => debug!(service, error = %e, "Public IP service unreachable"),
        }
    }
    None
}

struct WaitingGame {
    id: u32,
    name: String,
    owner: String,
    emulator: String,
    players: usize,
    max_players: u8,
}

async fn collect(state: &AppState) -> (usize, usize, Vec<WaitingGame>) {
    let users = state.clients_by_id.read().await.len();
    let mut waiting = Vec::new();
    let games = state.games.read().await;
    let total_games = games.len();
    for (&id, arc) in games.iter() {
        let game = arc.lock().await;
        if game.game_status == GAME_STATUS_WAITING {
            waiting.push(WaitingGame {
                id,
                name: String::from_utf8_lossy(&game.game_name).into_owned(),
                owner: String::from_utf8_lossy(&game.owner).into_owned(),
                emulator: String::from_utf8_lossy(&game.emulator_name).into_owned(),
                players: game.players.len(),
                max_players: game.max_players,
            });
        }
    }
    (users, total_games, waiting)
}

async fn report(client: &Client, state: &AppState, server_address: &str) {
    let config = &state.config;
    let ml = &config.master_list;
    let port = config.main_port.to_string();

    let (users, total_games, waiting) = collect(state).await;

    let users_s = users.to_string();
    let games_s = total_games.to_string();
    let max_users_s = ml.max_users.to_string();
    let max_games_s = ml.max_games.to_string();

    for server in ml.servers.iter() {
        let (url, protocol) = match &server.endpoint {
            MasterEndpoint::Preset { preset } => (preset.url().to_string(), preset.protocol()),
            MasterEndpoint::Custom { url, protocol } => (url.clone(), protocol.clone()),
        };

        match protocol {
            MasterProtocol::Kaillera => {
                touch_kaillera(
                    client,
                    &url,
                    ml,
                    server_address,
                    &port,
                    &users_s,
                    &games_s,
                    &max_users_s,
                    &waiting,
                )
                .await;
            }
            MasterProtocol::Emulinker => {
                touch_emulinker(
                    client,
                    &url,
                    ml,
                    server_address,
                    &port,
                    &users_s,
                    &games_s,
                    &max_users_s,
                    &max_games_s,
                    &waiting,
                )
                .await;
            }
        }
    }
}

async fn touch_kaillera(
    client: &Client,
    url: &str,
    ml: &MasterListConfig,
    server_address: &str,
    port: &str,
    users: &str,
    games: &str,
    max_users: &str,
    waiting: &[WaitingGame],
) {
    // format: {id}|{romName}|{ownerName}|{emulator}|{playerCount}|
    let wgames: String = waiting
        .iter()
        .map(|g| format!("{}|{}|{}|{}|{}|", g.id, g.name, g.owner, g.emulator, g.players))
        .collect();

    let result = client
        .get(url)
        .query(&[
            ("servername", ml.server_name.as_str()),
            ("port", port),
            ("nbusers", users),
            ("maxconn", max_users),
            ("version", VERSION),
            ("nbgames", games),
            ("location", ml.server_location.as_str()),
            ("ip", server_address),
            ("url", ml.server_website.as_str()),
        ])
        .header("Kaillera-games", "")
        .header("Kaillera-wgames", wgames)
        .send()
        .await;

    match result {
        Ok(resp) => debug!(url, status = resp.status().as_u16(), "Kaillera master touched"),
        Err(e) => warn!(url, error = %e, "Failed to touch Kaillera master"),
    }
}

async fn touch_emulinker(
    client: &Client,
    url: &str,
    ml: &MasterListConfig,
    server_address: &str,
    port: &str,
    users: &str,
    games: &str,
    max_users: &str,
    max_games: &str,
    waiting: &[WaitingGame],
) {
    // format: {romName}|{ownerName}|{emulator}|{playerCount}/{maxPlayers}|
    let wgames: String = waiting
        .iter()
        .map(|g| {
            format!(
                "{}|{}|{}|{}/{}|",
                g.name, g.owner, g.emulator, g.players, g.max_players
            )
        })
        .collect();

    let result = client
        .get(url)
        .query(&[
            ("serverName", ml.server_name.as_str()),
            ("ipAddress", server_address),
            ("location", ml.server_location.as_str()),
            ("website", ml.server_website.as_str()),
            ("port", port),
            ("numUsers", users),
            ("maxUsers", max_users),
            ("numGames", games),
            ("maxGames", max_games),
            ("version", VERSION),
        ])
        .header("Waiting-games", wgames)
        .send()
        .await;

    match result {
        Ok(resp) => debug!(url, status = resp.status().as_u16(), "EmuLinker master touched"),
        Err(e) => warn!(url, error = %e, "Failed to touch EmuLinker master"),
    }
}
