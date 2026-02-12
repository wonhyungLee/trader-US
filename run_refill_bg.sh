#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

/usr/bin/sqlite3 data/market_data.db "DELETE FROM refill_progress;"

PYBIN="$ROOT/.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

$PYBIN -u -m src.collectors.refill_loader \
  --chunk-days 150 \
  --start-mode listing \
  --sleep 0.1 \
  --notify-every 100 \
  --resume \
  > refill_debug.log 2>&1
