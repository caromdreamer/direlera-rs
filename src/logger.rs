// Logger configuration and setup
use std::io::IsTerminal;
use std::str::FromStr;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{
    layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

/// Configuration for pushing logs to a Loki endpoint. Built from config.toml and
/// passed to [`init_logger`]; `None` disables log push (stdout only).
pub struct LokiConfig {
    /// Base URL of the Loki server, e.g. "https://loki.example.com". The
    /// `/loki/api/v1/push` path is appended automatically.
    pub url: String,
    /// Unique server identifier, attached as a Loki label.
    pub server_id: String,
    /// Optional HTTP basic-auth username; when set, an Authorization header is
    /// sent with `password`.
    pub username: Option<String>,
    pub password: String,
}

/// Decide whether to emit ANSI color/style escape codes.
///
/// On Windows, legacy consoles don't interpret ANSI escapes and render them as
/// garbage (e.g. `←[2m`), which is what made the `pretty` format look broken.
/// We only enable ANSI when stdout is an interactive terminal, and on Windows
/// we additionally try to turn on virtual-terminal processing — if that fails
/// (old conhost), we fall back to plain, escape-free output.
fn ansi_enabled() -> bool {
    if !std::io::stdout().is_terminal() {
        return false;
    }
    #[cfg(windows)]
    {
        nu_ansi_term::enable_ansi_support().is_ok()
    }
    #[cfg(not(windows))]
    {
        true
    }
}

/// Initialize logger with different formats.
///
/// When `loki` is `Some`, a Loki push layer is added *alongside* the stdout
/// layer (push is additive — local `docker logs`/console output is preserved),
/// and the background ship task is spawned on the current Tokio runtime.
pub fn init_logger(format: LogFormat, level: LogLevel, loki: Option<LokiConfig>) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.as_str()));
    let ansi = ansi_enabled();

    // All formats are composed as boxed layers so an optional Loki layer can be
    // added on top regardless of the stdout format.
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();
    match format {
        LogFormat::Compact => layers.push(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(ansi)
                .boxed(),
        ),
        LogFormat::Pretty => layers.push(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(ansi)
                .pretty()
                .boxed(),
        ),
        LogFormat::Json => {
            // BunyanFormattingLayer propagates parent span fields into each log event,
            // producing flat JSON where session_id / game_id appear at the top level.
            // JsonStorageLayer must precede it to capture those span fields.
            layers.push(JsonStorageLayer.boxed());
            layers.push(BunyanFormattingLayer::new("direlera-rs".into(), std::io::stdout).boxed());
        }
    }

    // Optional Loki push layer. Errors here only disable log push — they must not
    // take down logging, so we warn via stderr (the subscriber isn't up yet).
    let mut bg_task = None;
    if let Some(cfg) = loki {
        // The caller only constructs `loki` when server_id is non-empty (it gates
        // the central collector's stream label), so it can be used directly.
        let server_id = cfg.server_id.clone();
        match build_loki_layer(&cfg, &server_id) {
            Ok((layer, task)) => {
                layers.push(layer.boxed());
                bg_task = Some(task);
            }
            Err(e) => {
                eprintln!("Failed to initialize Loki log push (logs will be stdout-only): {e}")
            }
        }
    }

    // Layers first (typed as `Layer<Registry>`), then the global EnvFilter on top.
    Registry::default().with(layers).with(filter).init();

    // The ship task must run for logs to actually be delivered. init_logger is
    // called from within the Tokio runtime (async main), so spawn is valid here.
    if let Some(task) = bg_task {
        tokio::spawn(task);
    }
}

/// Build the Loki layer + background ship task from config. Errors are unified to
/// `String` so URL-parse and builder errors can be reported the same way.
fn build_loki_layer(
    cfg: &LokiConfig,
    server_id: &str,
) -> Result<(tracing_loki::Layer, tracing_loki::BackgroundTask), String> {
    let url = tracing_loki::url::Url::parse(&cfg.url)
        .map_err(|e| format!("invalid logs_push_url '{}': {e}", cfg.url))?;

    // Keep label cardinality low: only stable identifiers belong in labels;
    // everything else stays in the log body.
    let mut builder = tracing_loki::builder()
        .label("service_name", "direlera")
        .map_err(|e| e.to_string())?
        .label("server_id", server_id)
        .map_err(|e| e.to_string())?;

    if let Some(user) = &cfg.username {
        use base64::Engine;
        let token =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{}", cfg.password));
        builder = builder
            .http_header("Authorization", format!("Basic {token}"))
            .map_err(|e| e.to_string())?;
    }

    builder.build_url(url).map_err(|e| e.to_string())
}

/// Log format options
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum LogFormat {
    Compact,
    Pretty,
    Json,
}

impl FromStr for LogFormat {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pretty" => Ok(LogFormat::Pretty),
            "json" => Ok(LogFormat::Json),
            _ => Ok(LogFormat::Compact),
        }
    }
}

/// Log level options
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

impl FromStr for LogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "warn" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Ok(LogLevel::Info),
        }
    }
}
