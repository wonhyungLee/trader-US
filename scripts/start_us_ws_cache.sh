#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

PYBIN="./.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="$(command -v python3)"
fi

mkdir -p logs data

if pgrep -af "^.*python(3)?[[:space:]]+-u?[[:space:]]*-m[[:space:]]+src\.autotrade\.us_ws_cache$" >/dev/null 2>&1; then
  echo "us ws cache already running"
  exit 0
fi

# Fully detach from current session so process survives shell termination.
if command -v setsid >/dev/null 2>&1; then
  setsid "$PYBIN" -u -m src.autotrade.us_ws_cache >> logs/us_ws_cache.out 2>&1 < /dev/null &
else
  nohup "$PYBIN" -u -m src.autotrade.us_ws_cache >> logs/us_ws_cache.out 2>&1 < /dev/null &
fi
echo $! > data/us_ws_cache.pid
echo "started us ws cache (pid=$!)"
