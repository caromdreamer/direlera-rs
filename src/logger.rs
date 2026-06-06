// Logger configuration and setup
use std::io::IsTerminal;
use std::str::FromStr;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

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

/// Initialize logger with different formats
pub fn init_logger(format: LogFormat, level: LogLevel) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.as_str()));
    let ansi = ansi_enabled();

    match format {
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .with_ansi(ansi)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .with_ansi(ansi)
                .pretty()
                .init();
        }
        LogFormat::Json => {
            // BunyanFormattingLayer propagates parent span fields into each log event,
            // producing flat JSON where session_id / game_id appear at the top level.
            let subscriber = Registry::default()
                .with(filter)
                .with(JsonStorageLayer)
                .with(BunyanFormattingLayer::new(
                    "direlera-rs".into(),
                    std::io::stdout,
                ));
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set tracing subscriber");
        }
    }
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
