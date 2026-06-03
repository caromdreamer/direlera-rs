// Logger configuration and setup
use std::str::FromStr;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

/// Initialize logger with different formats
pub fn init_logger(format: LogFormat, level: LogLevel) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.as_str()));

    match format {
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
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
