#!/usr/bin/env bash
set -euo pipefail

pids="$(pgrep -f "^.*python(3)?[[:space:]]+-u?[[:space:]]*-m[[:space:]]+src\.autotrade\.us_ws_cache$" || true)"
if [ -z "$pids" ]; then
  echo "us ws cache is not running"
  exit 0
fi

echo "stopping us ws cache: $pids"
kill $pids
