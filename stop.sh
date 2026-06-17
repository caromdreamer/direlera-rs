#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$ROOT_DIR/direlera.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "direlera-rs is not running from this directory (no direlera.pid)."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if [[ -z "$PID" ]] || ! kill -0 "$PID" 2>/dev/null; then
  echo "Removing stale pid file."
  rm -f "$PID_FILE"
  exit 0
fi

echo "Stopping direlera-rs (pid $PID)..."
kill "$PID"

for _ in {1..10}; do
  if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "Stopped."
    exit 0
  fi
  sleep 1
done

echo "Process did not stop after 10 seconds; forcing it."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "Stopped."
