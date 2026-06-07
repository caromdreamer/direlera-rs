use direlera_rs::logger::{init_logger, LogFormat, LogLevel, LokiConfig};
use packet_util::*;
use serde::Deserialize;
use std::fs;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod fields;
mod kaillera;
mod packet_util;
mod state;

mod handlers;
use handlers::*;

mod master_list;
mod session_manager;

mod simplest_game_sync;
use session_manager::SessionManager;
use state::*;

// Configuration structures
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_main_port")]
    pub main_port: u16,
    #[serde(default = "default_sub_port")]
    pub control_port: u16,
    #[serde(default)]
    pub tracing: TracingConfig,
    #[serde(default = "default_welcome_message")]
    pub welcome_message: String,
    #[serde(default)]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    /// Unique identifier for this server, attached as a label to pushed metrics
    /// (and, later, logs) so a central collector can tell servers apart. Must be
    /// unique across all servers reporting to the same backend.
    #[serde(default)]
    pub server_id: String,
    /// When true, push metrics to a Prometheus Pushgateway instead of exposing
    /// the local scrape endpoint. Mutually exclusive with the scrape listener:
    /// enabling push disables `metrics_port`. Designed for servers behind
    /// NAT/firewall that a central Prometheus cannot scrape directly.
    #[serde(default)]
    pub metrics_push_enabled: bool,
    /// Base URL of the Pushgateway (typically behind an HTTPS reverse proxy),
    /// e.g. "https://metrics.example.com". The job/server_id path is appended
    /// automatically.
    #[serde(default)]
    pub metrics_push_url: String,
    /// How often (seconds) to push metrics to the gateway.
    #[serde(default = "default_metrics_push_interval")]
    pub metrics_push_interval_secs: u64,
    /// Optional HTTP basic-auth credentials for the push endpoint.
    #[serde(default)]
    pub metrics_push_username: String,
    #[serde(default)]
    pub metrics_push_password: String,
    /// When true, push logs to a Loki endpoint (in addition to stdout). Designed,
    /// like metrics push, for servers a central collector cannot reach to scrape.
    #[serde(default)]
    pub logs_push_enabled: bool,
    /// Base URL of the Loki server, e.g. "https://loki.example.com".
    #[serde(default)]
    pub logs_push_url: String,
    /// Optional HTTP basic-auth credentials for the Loki push endpoint.
    #[serde(default)]
    pub logs_push_username: String,
    #[serde(default)]
    pub logs_push_password: String,
    #[serde(default)]
    pub master_list: MasterListConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            main_port: default_main_port(),
            control_port: default_sub_port(),
            tracing: TracingConfig::default(),
            welcome_message: default_welcome_message(),
            metrics_enabled: false,
            metrics_port: default_metrics_port(),
            server_id: String::new(),
            metrics_push_enabled: false,
            metrics_push_url: String::new(),
            metrics_push_interval_secs: default_metrics_push_interval(),
            metrics_push_username: String::new(),
            metrics_push_password: String::new(),
            logs_push_enabled: false,
            logs_push_url: String::new(),
            logs_push_username: String::new(),
            logs_push_password: String::new(),
            master_list: MasterListConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TracingConfig {
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default = "default_level")]
    pub level: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            format: default_format(),
            level: default_level(),
        }
    }
}

// ── Master server list ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct MasterListConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub server_name: String,
    /// Public IP or hostname that clients use to connect.
    #[serde(default)]
    pub server_address: String,
    #[serde(default)]
    pub server_location: String,
    #[serde(default)]
    pub server_website: String,
    #[serde(default = "master_default_max_users")]
    pub max_users: u32,
    #[serde(default = "master_default_max_games")]
    pub max_games: u32,
    /// List of master servers to report to. Defaults to the two official servers
    /// when omitted. Add any number of entries to report to additional endpoints.
    #[serde(default = "default_master_servers")]
    pub servers: Vec<MasterServerConfig>,
}

impl Default for MasterListConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_name: String::new(),
            server_address: String::new(),
            server_location: String::new(),
            server_website: String::new(),
            max_users: master_default_max_users(),
            max_games: master_default_max_games(),
            servers: default_master_servers(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MasterServerConfig {
    #[serde(flatten)]
    pub endpoint: MasterEndpoint,
}

/// Either a named preset (URL + protocol bundled) or a fully custom entry.
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum MasterEndpoint {
    Preset {
        preset: MasterPreset,
    },
    Custom {
        url: String,
        protocol: MasterProtocol,
    },
}

/// Built-in named servers — no URL to memorize.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum MasterPreset {
    /// http://www.kaillera.com/touch_server.php
    Kaillera,
    /// http://kaillerareborn.2manygames.fr/touch_list.php
    Emulinker,
}

impl MasterPreset {
    pub fn url(&self) -> &'static str {
        match self {
            MasterPreset::Kaillera => "http://www.kaillera.com/touch_server.php",
            MasterPreset::Emulinker => "http://kaillerareborn.2manygames.fr/touch_list.php",
        }
    }

    pub fn protocol(&self) -> MasterProtocol {
        match self {
            MasterPreset::Kaillera => MasterProtocol::Kaillera,
            MasterPreset::Emulinker => MasterProtocol::Emulinker,
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MasterProtocol {
    /// kaillera.com-style: query params servername/nbusers/ip/…
    Kaillera,
    /// EmuLinkerReborn-style: query params serverName/numUsers/ipAddress/…
    Emulinker,
}

fn default_master_servers() -> Vec<MasterServerConfig> {
    vec![]
}

fn master_default_max_users() -> u32 {
    100
}

fn master_default_max_games() -> u32 {
    50
}

// ────────────────────────────────────────────────────────────────────────────

fn default_metrics_port() -> u16 {
    9091
}

fn default_metrics_push_interval() -> u64 {
    15
}

fn default_main_port() -> u16 {
    8080
}

fn default_sub_port() -> u16 {
    27888
}

fn default_format() -> String {
    "compact".to_string()
}

fn default_level() -> String {
    "info".to_string()
}

fn default_welcome_message() -> String {
    "Welcome to the Kaillera server!".to_string()
}

// Load configuration from config.toml
fn load_config() -> Config {
    match fs::read_to_string("config.toml") {
        Ok(contents) => match toml::from_str(&contents) {
            Ok(config) => {
                eprintln!("Configuration loaded from config.toml");
                config
            }
            Err(e) => {
                eprintln!("Failed to parse config.toml: {}", e);
                eprintln!("Using default configuration");
                Config::default()
            }
        },
        Err(_) => {
            eprintln!("config.toml not found.");
            eprintln!("Copy config.toml.example to config.toml to customize:");
            eprintln!("  cp config.toml.example config.toml");
            eprintln!("Using default configuration for now.");
            Config::default()
        }
    }
}
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    // Load configuration from config.toml
    let config = load_config();

    // Parse log level
    let log_level = match config.tracing.level.to_lowercase().as_str() {
        "trace" => LogLevel::Trace,
        "debug" => LogLevel::Debug,
        "info" => LogLevel::Info,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => {
            eprintln!("Invalid log level '{}', using INFO", config.tracing.level);
            LogLevel::Info
        }
    };

    // Initialize tracing subscriber based on config
    let log_format = match config.tracing.format.to_lowercase().as_str() {
        "pretty" => LogFormat::Pretty,
        "json" => LogFormat::Json,
        "compact" => LogFormat::Compact,
        _ => LogFormat::Compact,
    };

    // Build optional Loki push config (additive to stdout). Disabled when the
    // toggle is off or no URL is set.
    let loki = if config.logs_push_enabled && !config.logs_push_url.is_empty() {
        Some(LokiConfig {
            url: config.logs_push_url.clone(),
            server_id: config.server_id.clone(),
            username: (!config.logs_push_username.is_empty())
                .then(|| config.logs_push_username.clone()),
            password: config.logs_push_password.clone(),
        })
    } else {
        if config.logs_push_enabled && config.logs_push_url.is_empty() {
            eprintln!("logs_push_enabled = true but logs_push_url is empty — log push disabled");
        }
        None
    };

    init_logger(log_format, log_level, loki);

    // Buckets in seconds. Without explicit buckets the exporter emits summary
    // type instead of histogram, which breaks histogram_quantile() in PromQL.
    let buckets = &[
        0.000005, // 5µs
        0.00001,  // 10µs
        0.00002,  // 20µs
        0.00005,  // 50µs
        0.0001,   // 100µs
        0.0002,   // 200µs
        0.0005,   // 500µs
        0.001,    // 1ms
        0.005,    // 5ms
        0.01,     // 10ms
        0.05,     // 50ms
        0.1,      // 100ms
        0.5,      // 500ms
    ];
    // The pace ratio is dimensionless (~1.0 = on pace, 2.0 = half speed), so it
    // needs its own buckets — the time-based defaults above (5µs–500ms) would
    // dump every ratio sample into +Inf.
    let ratio_buckets = &[0.8, 0.9, 1.0, 1.1, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0];
    // Input arrival interval — frame-period resolution so common frame rates land
    // in distinct buckets (60fps≈16.7ms, 50fps=20ms, 30fps≈33.3ms, 20fps=50ms)
    // and slowdowns are visible. The default buckets jump 10ms→50ms, lumping all
    // playable rates into one bucket.
    let interval_buckets = &[
        0.004, 0.008, 0.012, 0.016, 0.02, 0.025, 0.033, 0.04, 0.05, 0.066, 0.1, 0.2, 0.5,
    ];
    // Push mode requires a destination URL; warn and fall back to off if missing.
    let push_ok = config.metrics_push_enabled && !config.metrics_push_url.is_empty();
    if config.metrics_push_enabled && config.metrics_push_url.is_empty() {
        warn!("metrics_push_enabled = true but metrics_push_url is empty — metrics push disabled");
    }

    if push_ok || config.metrics_enabled {
        // Builder config shared by both modes (buckets + idle expiry).
        let builder = metrics_exporter_prometheus::PrometheusBuilder::new()
            .set_buckets(buckets)
            .expect("Failed to set histogram buckets")
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full("client_input_pace_ratio".to_string()),
                ratio_buckets,
            )
            .expect("Failed to set pace-ratio buckets")
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full(
                    "client_input_interval_seconds".to_string(),
                ),
                interval_buckets,
            )
            .expect("Failed to set input-interval buckets")
            // Per-game metrics are labeled by game_id, so their series must not
            // accumulate forever. Expire histograms idle for 5 min: ended games
            // get reclaimed, while in-progress games update constantly and stay.
            .idle_timeout(
                metrics_util::MetricKindMask::HISTOGRAM,
                Some(std::time::Duration::from_secs(300)),
            );

        // Push and scrape-listener are mutually exclusive in the exporter; push
        // wins when enabled (the unified path for NAT/firewalled servers).
        if push_ok {
            let server_id = if config.server_id.is_empty() {
                warn!(
                    "server_id is empty; pushing metrics as 'unknown'. \
                     Set a unique server_id in config.toml."
                );
                "unknown".to_string()
            } else {
                config.server_id.clone()
            };
            // Build the full Pushgateway grouping path: server_id becomes a label
            // and must be unique per server, else gateways overwrite each other's
            // group. We push via reqwest (already a dependency, ring-backed) rather
            // than the exporter's built-in push-gateway feature, which forces an
            // aws-lc-rs/cmake build chain that breaks the slim Docker image.
            let base = config.metrics_push_url.trim_end_matches('/');
            let endpoint = format!("{base}/metrics/job/direlera/server_id/{server_id}");
            let username = (!config.metrics_push_username.is_empty())
                .then(|| config.metrics_push_username.clone());
            let password = config.metrics_push_password.clone();
            let interval = std::time::Duration::from_secs(config.metrics_push_interval_secs);

            let handle = builder
                .install_recorder()
                .expect("Failed to install Prometheus recorder");
            // Bound each request by the push interval so a hung/slow endpoint
            // can't wedge the task — which would also stall run_upkeep() and let
            // idle histograms accumulate. A failed push just retries next tick.
            let client = reqwest::Client::builder()
                .timeout(interval)
                .build()
                .expect("Failed to build metrics push HTTP client");
            info!(
                endpoint = endpoint.as_str(),
                interval_secs = config.metrics_push_interval_secs,
                server_id = server_id.as_str(),
                "Prometheus metrics push started"
            );

            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    handle.run_upkeep();
                    let body = handle.render();
                    // PUT replaces the whole group each push, so a restarted/stale
                    // server's metrics get overwritten rather than duplicated.
                    let mut req = client.put(&endpoint).body(body);
                    if let Some(user) = &username {
                        req = req.basic_auth(user, Some(&password));
                    }
                    match req.send().await {
                        Ok(resp) if resp.status().is_success() => {}
                        Ok(resp) => warn!(
                            status = %resp.status(),
                            "metrics push returned non-success status"
                        ),
                        Err(e) => warn!(error = %e, "failed to push metrics"),
                    }
                }
            });
        } else {
            builder
                .with_http_listener(([0, 0, 0, 0], config.metrics_port))
                .install()
                .expect("Failed to start Prometheus metrics exporter");
            info!(
                port = config.metrics_port,
                "Prometheus metrics exporter started"
            );
        }
    } else {
        info!("Prometheus metrics exporter disabled");
    }

    metrics::gauge!("active_sessions_total").set(0.0);
    metrics::gauge!("active_games_total").set(0.0);

    let git_commit = std::env::var("GIT_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    info!(git_commit = git_commit.as_str(), "Server starting");

    info!(
        { fields::CONFIG_SOURCE } = "config.toml",
        { fields::PORT } = config.main_port,
        control_port = config.control_port,
        tracing_format = config.tracing.format.as_str(),
        tracing_level = config.tracing.level.as_str(),
        "Server configuration loaded"
    );

    // Bind two UDP sockets using configured ports
    let main_socket = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{}", config.main_port))
            .await
            .map_err(|e| {
                error!(
                    { fields::PORT } = config.main_port,
                    { fields::ERROR } = %e,
                    "Failed to bind main socket"
                );
                e
            })?,
    );

    let control_socket = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{}", config.control_port))
            .await
            .map_err(|e| {
                error!(
                    { fields::PORT } = config.control_port,
                    { fields::ERROR } = %e,
                    "Failed to bind control socket"
                );
                e
            })?,
    );

    info!(
        { fields::PORT } = config.main_port,
        control_port = config.control_port,
        bind_address = "0.0.0.0",
        "Sockets bound successfully - server listening"
    );

    let (tx, mut rx) = mpsc::channel::<Message>(100);

    // Centralized AppState with RwLock for efficiency (shared by all sessions)
    let global_state = Arc::new(AppState::new(tx.clone(), config.clone()));

    // Initialize Session Manager for TCP-like session handling
    let (session_manager, packet_rx) = SessionManager::new();
    let session_manager = Arc::new(session_manager);

    // Start periodic session cleanup task
    session_manager
        .clone()
        .start_cleanup_task(global_state.clone());

    // Start stall recovery task — resends the last packet to stalled players
    session_manager::start_stall_resend_task(global_state.clone());

    // Start session manager (spawns handlers for each client)
    let manager_for_run = session_manager.clone();
    let state_for_sessions = global_state.clone();
    tokio::spawn(async move {
        manager_for_run.run(packet_rx, state_for_sessions).await;
    });

    tokio::spawn(master_list::run(global_state.clone()));

    info!("Server initialization complete");

    // Task to send responses in the main socket
    let main_socket_send = main_socket.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let Err(e) = main_socket_send.send_to(&message.data, &message.addr).await {
                warn!(
                    { fields::ADDR } = %message.addr,
                    { fields::ERROR } = %e,
                    "Failed to send response"
                );
            }
        }
    });

    // Control logic for HELLO0.83 and PING requests (Port 27888)
    let main_port_for_control = config.main_port;
    tokio::spawn(handle_control_socket(
        control_socket.clone(),
        main_port_for_control,
    ));

    info!("Server ready to accept connections");

    // Main UDP dispatcher - forwards packets to session manager
    let packet_sender = session_manager.packet_sender();
    let mut buf = [0u8; 4096];

    loop {
        let recv_result = main_socket.recv_from(&mut buf).await;
        let (len, src) = match recv_result {
            Ok(ok) => ok,
            Err(e) => {
                // recv_from errors are usually system-level issues, not client-specific
                // Log at debug level to avoid spam, as these are often expected
                debug!(
                    { fields::ERROR } = %e,
                    "recv_from failed, continuing"
                );
                continue;
            }
        };
        let data = buf[..len].to_vec();

        // PING probe — respond immediately without creating a session
        if data == b"PING\x00" {
            let _ = main_socket.send_to(b"PONG\x00", src).await;
            continue;
        }

        // Forward to session manager (will create session if needed)
        if let Err(e) = packet_sender.send((src, data)).await {
            warn!(
                { fields::ADDR } = %src,
                { fields::ERROR } = %e,
                "Failed to forward packet to session manager"
            );
        }
    }
}

// Message struct needs to be accessible in both files
pub struct Message {
    pub data: Vec<u8>,
    pub addr: std::net::SocketAddr,
}

/// Process a single packet within a session.
/// `packet_counter` is per-session local state tracking the next expected message number.
async fn process_packet_in_session(
    data: Vec<u8>,
    addr: std::net::SocketAddr,
    global_state: Arc<AppState>,
    packet_counter: &mut u16,
) {
    // This fn runs inside the session span (see handle_session .instrument(span)),
    // so the current span here is that long-lived session span.
    let session_span = tracing::Span::current();
    match parse_packet(&data) {
        Ok(messages) => {
            for message in messages.iter() {
                // Message number 0 signals the start of a new sequence
                if message.message_number == 0 && messages.len() == 1 {
                    *packet_counter = 0;
                }
            }

            let had_messages = !messages.is_empty();
            let mut processed = 0u32;
            for message in messages {
                let message_number_to_process = *packet_counter;

                if message.message_number == message_number_to_process {
                    *packet_counter = message_number_to_process + 1;
                    processed += 1;

                    let msg_number = message.message_number;
                    let msg_type = message.message_type;

                    if let Err(e) =
                        handle_message(message, &addr, global_state.clone(), session_span.clone())
                            .await
                    {
                        error!(
                            { fields::MESSAGE_NUMBER } = msg_number,
                            { fields::MESSAGE_TYPE } = format!("0x{:02X}", msg_type),
                            error = ?e,
                            error_chain = %e,
                            "Failed to handle message"
                        );
                    }
                }
            }

            // A packet that yielded zero newly-processed messages (all of its
            // messages were already-seen duplicates) is the client re-sending its
            // last bundle while it waits on the server — a "0 new messages" stall
            // signal. We recover via the server-side stall-resend task, so here we
            // only meter how often this happens to gauge whether a pull-based
            // resend (acting directly on this signal) would be worth adding.
            if had_messages && processed == 0 {
                metrics::counter!("inbound_zero_new_packets_total").increment(1);
            }
        }
        Err(e) => {
            let preview = if !data.is_empty() {
                format!("{:02x?}", &data[..data.len().min(20)])
            } else {
                "empty".to_string()
            };
            warn!(
                { fields::PACKET_SIZE } = data.len(),
                { fields::ERROR } = %e,
                packet_preview = preview,
                "Failed to parse packet"
            );
        }
    }
}
