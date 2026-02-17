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

# Detect only real worker command line; avoid matching current shell command text.
if pgrep -af "^.*python(3)?[[:space:]]+-u[[:space:]]+-m[[:space:]]+src\\.autotrade\\.worker$" >/dev/null 2>&1; then
  echo "autotrade worker already running"
  exit 0
fi

nohup "$PYBIN" -u -m src.autotrade.worker >> logs/autotrade_worker.out 2>&1 &
echo $! > data/autotrade_worker.pid
echo "started autotrade worker (pid=$!)"
