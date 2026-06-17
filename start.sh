#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$ROOT_DIR/direlera.pid"
LOG_FILE="$ROOT_DIR/direlera.log"
BIN="$ROOT_DIR/target/release/direlera-rs"

cd "$ROOT_DIR"

if [[ "${1:-}" == "--build" || ! -x "$BIN" ]]; then
  echo "Building direlera-rs..."
  cargo build --release
fi

if [[ ! -f "$ROOT_DIR/config.toml" ]]; then
  echo "config.toml not found; copying config.toml.example"
  cp "$ROOT_DIR/config.toml.example" "$ROOT_DIR/config.toml"
fi

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE")"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "direlera-rs is already running (pid $OLD_PID)."
    echo "Use ./stop.sh first if you want to restart it."
    exit 0
  fi
  rm -f "$PID_FILE"
fi

echo "Starting direlera-rs..."
nohup "$BIN" >"$LOG_FILE" 2>&1 &
PID="$!"
echo "$PID" >"$PID_FILE"

sleep 1
if kill -0 "$PID" 2>/dev/null; then
  echo "Started direlera-rs (pid $PID)."
  echo "Logs: $LOG_FILE"
else
  rm -f "$PID_FILE"
  echo "direlera-rs failed to start. Last log lines:"
  tail -n 40 "$LOG_FILE" || true
  exit 1
fi
