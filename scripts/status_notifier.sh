#!/usr/bin/env bash
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"
export PYTHONUNBUFFERED=1
INTERVAL_SEC=${STATUS_NOTIFY_INTERVAL_SEC:-600}
PYBIN="$ROOT/.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
  PYBIN="python3"
fi

while true; do
  if ! "$PYBIN" - <<'PY'
import sqlite3, time
from src.utils.config import load_settings
from src.utils.notifier import maybe_notify
settings = load_settings()
conn = sqlite3.connect('data/market_data.db')
conn.row_factory = sqlite3.Row

total = conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0] or 0
done = conn.execute(
    "SELECT COUNT(*) FROM refill_progress WHERE status='DONE' AND code IN (SELECT code FROM universe_members)"
).fetchone()[0] or 0
remaining = max(total - done, 0)
pct = (done / total * 100.0) if total else 0.0
last_update = conn.execute("SELECT MAX(updated_at) FROM refill_progress").fetchone()[0]
job = conn.execute(
    "SELECT status, started_at, finished_at FROM job_runs WHERE job_name='refill_loader' ORDER BY id DESC LIMIT 1"
).fetchone()

ts = time.strftime("%Y-%m-%d %H:%M:%S")
job_msg = ""
if job:
    job_msg = f"last_job={job[0]} started={job[1]} finished={job[2]}"

state = "DONE" if total and done >= total else "RUNNING"
msg = (
    f"[REFILL STATUS] {ts}\n"
    f"state={state} done {done}/{total} ({pct:.1f}%) remaining {remaining}\n"
    f"last_update={last_update}\n"
    f"{job_msg}\n"
)

maybe_notify(settings, msg)
print(msg, flush=True)
PY
  then
    echo "[WARN] status_notifier python failed" >&2
  fi
  sleep "$INTERVAL_SEC"
done
