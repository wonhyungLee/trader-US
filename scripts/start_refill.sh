#!/usr/bin/env bash
set -e
# Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$DIR")"
cd "$PARENT_DIR"

echo "Starting refill at $(date)"
export PYTHONUNBUFFERED=1
export PYTHONPATH="$PARENT_DIR"

PYBIN="$PARENT_DIR/.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

# Universe는 DB의 universe_members를 사용합니다 (universe_loader 선행 필요).
$PYBIN -u -m src.collectors.refill_loader \
  --chunk-days 150 \
  --start-mode listing \
  --sleep 0.1 \
  --resume

echo "Refill script exited with $?"
