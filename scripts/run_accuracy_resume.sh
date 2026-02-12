#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONUNBUFFERED=1

PYBIN="$ROOT/.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

# Note: accuracy_data_loader는 국내 지표 수집용입니다.
$PYBIN -u -m src.collectors.accuracy_data_loader \
  --resume \
  --notify-every 5
