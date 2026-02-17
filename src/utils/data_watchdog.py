from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from src.utils.config import load_settings
from src.utils.notifier import maybe_notify
from src.utils.project_root import ensure_repo_root

# NOTE:
# This repo is the US "viewer" app. Historical KR-only "accuracy" refill tables
# existed in older projects, but they do not apply to US tickers and can create
# infinite refill loops. Keep the feature behind a flag (default: off).
ACCURACY_TABLES = {
    "investor_flow_daily": "inv",
    "program_trade_daily": "prog",
    "short_sale_daily": "short",
    "credit_balance_daily": "credit",
    "loan_trans_daily": "loan",
    "vi_status_daily": "vi",
}


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(path: Path, state: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _lock_active(lock_path: Path) -> bool:
    if not lock_path.exists():
        return False
    try:
        pid = int(lock_path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        pid = 0
    if _pid_alive(pid):
        return True
    try:
        lock_path.unlink()
    except Exception:
        pass
    return False


def _get_last_price_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM daily_price").fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def _missing_codes_for_date(conn: sqlite3.Connection, table: str, date: str) -> List[str]:
    sql = (
        f"SELECT u.code "
        f"FROM universe_members u "
        f"LEFT JOIN {table} t ON u.code=t.code AND t.date=? "
        f"WHERE t.code IS NULL"
    )
    rows = conn.execute(sql, (date,)).fetchall()
    return [r[0] for r in rows]


def _missing_codes_any(conn: sqlite3.Connection, table: str) -> List[str]:
    """Codes that have zero rows in `table`."""
    sql = (
        f"SELECT u.code "
        f"FROM universe_members u "
        f"LEFT JOIN (SELECT DISTINCT code FROM {table}) t ON u.code=t.code "
        f"WHERE t.code IS NULL "
        f"ORDER BY u.code"
    )
    rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]


def _invalid_latest_codes(
    conn: sqlite3.Connection,
    date_str: str,
    amount_floor: float,
    volume_floor: float,
) -> List[str]:
    try:
        rows = conn.execute(
            """
            SELECT code
            FROM daily_price
            WHERE date = ?
              AND (CAST(COALESCE(amount, 0) AS REAL) <= ? OR CAST(COALESCE(volume, 0) AS REAL) <= ?)
            ORDER BY code
            """,
            (date_str, amount_floor, volume_floor),
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _invalid_codes_in_window(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    amount_floor: float,
    volume_floor: float,
) -> List[str]:
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT u.code
            FROM universe_members u
            JOIN daily_price dp
              ON u.code = dp.code
            WHERE dp.date BETWEEN ? AND ?
              AND (
                  CAST(COALESCE(dp.amount, 0) AS REAL) <= ?
                  OR CAST(COALESCE(dp.volume, 0) AS REAL) <= ?
              )
            ORDER BY u.code
            """,
            (start_date, end_date, amount_floor, volume_floor),
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _count_codes_for_date(conn: sqlite3.Connection, table: str, date_str: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table} WHERE date=?", (date_str,)).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _list_universe_codes(conn: sqlite3.Connection, limit: Optional[int] = None) -> List[str]:
    try:
        if limit is not None and int(limit) > 0:
            rows = conn.execute("SELECT code FROM universe_members ORDER BY code LIMIT ?", (int(limit),)).fetchall()
        else:
            rows = conn.execute("SELECT code FROM universe_members ORDER BY code").fetchall()
    except Exception:
        return []
    out: List[str] = []
    for row in rows:
        if not row or not row[0]:
            continue
        code = str(row[0]).strip().upper()
        if code:
            out.append(code)
    return out


def _probe_market_date_et(after_close_min: int) -> Optional[str]:
    """Return ET market date to probe for close data, or None if not in probe window."""
    try:
        now_et = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return None
    if now_et.weekday() >= 5:  # weekend
        return None
    cutoff_min = (16 * 60) + max(0, int(after_close_min))
    now_min = now_et.hour * 60 + now_et.minute
    if now_min < cutoff_min:
        return None
    return now_et.date().isoformat()


def _write_codes_csv(path: Path, codes: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("code\n")
        for c in codes:
            f.write(f"{c}\n")


def _run_accuracy_refill(
    date: str,
    codes_file: Path,
    progress_file: Path,
    lock_file: Path,
    notify_every: int,
    item_sleep: float,
    rate_sleep: Optional[float],
    limit: Optional[int],
):
    cmd = [
        sys.executable,
        "-m",
        "src.collectors.accuracy_data_loader",
        "--start-date",
        date,
        "--end-date",
        date,
        "--codes-file",
        str(codes_file),
        "--notify-every",
        str(notify_every),
        "--sleep",
        str(item_sleep),
        "--progress-file",
        str(progress_file),
        "--lock-file",
        str(lock_file),
    ]
    if rate_sleep is not None:
        cmd.extend(["--rate-sleep", str(rate_sleep)])
    if limit is not None and limit > 0:
        cmd.extend(["--limit", str(limit)])

    logging.info("Running accuracy refill: %s", " ".join(cmd))
    res = subprocess.run(cmd, check=False)
    return res.returncode


def _run_daily_refill(
    chunk_days: int,
    limit: Optional[int] = None,
    codes_file: Optional[str] = None,
    *,
    include_latest: bool = False,
    include_latest_days: int = 0,
):
    cmd = [
        sys.executable,
        "-m",
        "src.collectors.daily_loader",
        "--chunk-days",
        str(chunk_days),
    ]
    if codes_file:
        cmd.extend(["--codes-file", str(codes_file)])
    if limit is not None and int(limit) > 0:
        cmd.extend(["--limit", str(int(limit))])
    if include_latest:
        cmd.append("--include-latest")
    if include_latest_days > 0:
        cmd.extend(["--include-latest-days", str(int(include_latest_days))])
    logging.info("Running daily refill: %s", " ".join(cmd))
    res = subprocess.run(cmd, check=False)
    return res.returncode


def _load_cfg(settings: dict, args) -> Dict[str, object]:
    wd = settings.get("watchdog", {})
    def _get(name: str, default):
        return wd.get(name, default)

    interval = float(args.interval if args.interval is not None else _get("interval_sec", 1800))
    accuracy_enabled = bool(_get("accuracy_enabled", False))
    cooldown = float(args.cooldown if args.cooldown is not None else _get("accuracy_cooldown_sec", 21600))
    min_missing = int(args.min_missing if args.min_missing is not None else _get("accuracy_min_missing", 1))
    daily_min_missing = int(args.daily_min_missing if args.daily_min_missing is not None else _get("daily_min_missing", 1))
    daily_chunk_days = int(args.daily_chunk_days if args.daily_chunk_days is not None else _get("daily_chunk_days", 90))
    daily_stale_days = int(args.daily_stale_days if args.daily_stale_days is not None else _get("daily_stale_days", 1))
    if daily_stale_days < 0:
        daily_stale_days = 0
    invalid_latest_enabled = bool(_get("invalid_latest_enabled", True))
    invalid_latest_amount_threshold = float(_get("invalid_latest_amount_threshold", 0))
    invalid_latest_volume_threshold = float(_get("invalid_latest_volume_threshold", 0))
    daily_limit_raw = args.daily_limit if args.daily_limit is not None else _get("daily_limit", 20)
    daily_limit = int(daily_limit_raw) if daily_limit_raw is not None else None
    if daily_limit is not None and daily_limit <= 0:
        daily_limit = None
    daily_cooldown = float(args.daily_cooldown if args.daily_cooldown is not None else _get("daily_cooldown_sec", 7200))
    if args.daily_enabled is None:
        daily_enabled = bool(_get("daily_enabled", True))
    else:
        daily_enabled = bool(args.daily_enabled)
    market_probe_enabled = bool(_get("market_probe_enabled", True))
    market_probe_after_close_min = int(_get("market_probe_after_close_min", 45))
    if market_probe_after_close_min < 0:
        market_probe_after_close_min = 0
    market_probe_retry_limit = int(_get("market_probe_retry_limit", 6))
    if market_probe_retry_limit < 1:
        market_probe_retry_limit = 1
    market_probe_sample_size = int(_get("market_probe_sample_size", 20))
    if market_probe_sample_size < 1:
        market_probe_sample_size = 1
    notify_every = int(args.notify_every if args.notify_every is not None else _get("accuracy_notify_every", 20))
    item_sleep = float(args.item_sleep if args.item_sleep is not None else _get("accuracy_item_sleep_sec", 0.5))
    rate_sleep = args.rate_sleep if args.rate_sleep is not None else _get("accuracy_rate_sleep_sec", None)
    if rate_sleep is not None:
        rate_sleep = float(rate_sleep)

    progress_file = Path(args.progress_file or _get("accuracy_progress_file", "data/accuracy_progress_watchdog.json"))
    codes_file = Path(args.codes_file or _get("daily_codes_file", "data/csv/daily_missing_codes.csv"))
    accuracy_lock = Path(args.accuracy_lock_file or _get("accuracy_lock_file", "data/accuracy_loader.lock"))
    daily_lock = Path(args.daily_lock_file or _get("daily_lock_file", "data/daily_loader.lock"))
    refill_lock = Path(args.refill_lock_file or _get("refill_lock_file", "data/locks/refill_loader.lock"))
    lock_file = Path(args.lock_file or _get("lock_file", "data/watchdog.lock"))
    state_file = Path(args.state_file or "data/watchdog_state.json")

    return {
        "interval": interval,
        "accuracy_enabled": accuracy_enabled,
        "cooldown": cooldown,
        "min_missing": min_missing,
        "daily_min_missing": daily_min_missing,
        "daily_chunk_days": daily_chunk_days,
        "daily_stale_days": daily_stale_days,
        "daily_limit": daily_limit,
        "daily_cooldown": daily_cooldown,
        "daily_enabled": daily_enabled,
        "market_probe_enabled": market_probe_enabled,
        "market_probe_after_close_min": market_probe_after_close_min,
        "market_probe_retry_limit": market_probe_retry_limit,
        "market_probe_sample_size": market_probe_sample_size,
        "notify_every": notify_every,
        "item_sleep": item_sleep,
        "rate_sleep": rate_sleep,
        "progress_file": progress_file,
        "codes_file": codes_file,
        "accuracy_lock_file": accuracy_lock,
        "daily_lock_file": daily_lock,
        "refill_lock_file": refill_lock,
        "lock_file": lock_file,
        "state_file": state_file,
        "limit": args.limit,
        "once": args.once,
        "no_refill": args.no_refill,
        "invalid_latest_enabled": invalid_latest_enabled,
        "invalid_latest_amount_threshold": invalid_latest_amount_threshold,
        "invalid_latest_volume_threshold": invalid_latest_volume_threshold,
    }


def run_once(settings: dict, cfg: Dict[str, object]) -> None:
    conn = sqlite3.connect("data/market_data.db")
    conn.row_factory = sqlite3.Row
    try:
        last_date = _get_last_price_date(conn)
        if not last_date:
            maybe_notify(settings, "[watchdog] no daily_price date found; skip")
            return

        # daily_price missing for last_date
        try:
            miss_daily = _missing_codes_for_date(conn, "daily_price", last_date)
            daily_missing_count = len(miss_daily)
        except sqlite3.OperationalError:
            miss_daily = []
            daily_missing_count = 0

        # daily_price missing entirely (bootstrap targets)
        try:
            miss_any = _missing_codes_any(conn, "daily_price")
        except sqlite3.OperationalError:
            miss_any = []

        invalid_latest_codes: List[str] = []
        invalid_latest_count = 0
        invalid_stale_count = 0
        invalid_window_codes: List[str] = []
        invalid_window_enabled = max(1, int(cfg.get("daily_stale_days", 1)))
        stale_start = None
        state_path: Path = cfg["state_file"]
        state = _read_state(state_path)

        if bool(cfg["invalid_latest_enabled"]) and last_date:
            invalid_latest_codes = _invalid_latest_codes(
                conn,
                last_date,
                float(cfg["invalid_latest_amount_threshold"]),
                float(cfg["invalid_latest_volume_threshold"]),
            )
            invalid_latest_count = len(invalid_latest_codes)
            parsed_last = datetime.strptime(last_date, "%Y-%m-%d").date()
            stale_start = (parsed_last - timedelta(days=invalid_window_enabled - 1)).isoformat()
            invalid_window_codes = _invalid_codes_in_window(
                conn,
                stale_start,
                last_date,
                float(cfg["invalid_latest_amount_threshold"]),
                float(cfg["invalid_latest_volume_threshold"]),
            )
            invalid_stale_count = len(invalid_window_codes)

        targets: List[str] = []
        seen = set()
        for c in (miss_any + miss_daily + invalid_latest_codes + invalid_window_codes):
            if c and c not in seen:
                seen.add(c)
                targets.append(c)

        probe_date: Optional[str] = None
        probe_codes_on_date = 0
        probe_force_run = False
        probe_next_attempt: Optional[int] = None
        if bool(cfg.get("market_probe_enabled", True)):
            probe_date = _probe_market_date_et(int(cfg.get("market_probe_after_close_min", 45)))
            if probe_date:
                if str(state.get("market_probe_date") or "") != probe_date:
                    state["market_probe_date"] = probe_date
                    state["market_probe_attempts"] = 0
                    state["market_probe_holiday"] = False
                    state.pop("market_probe_holiday_at", None)
                    state.pop("market_probe_detected_at", None)
                    state.pop("market_probe_last_attempt_at", None)
                    state.pop("market_probe_reason", None)

                if str(last_date) < probe_date:
                    probe_codes_on_date = _count_codes_for_date(conn, "daily_price", probe_date)
                    state["market_probe_rows"] = probe_codes_on_date
                    if probe_codes_on_date > 0:
                        state["market_probe_holiday"] = False
                        state["market_probe_detected_at"] = datetime.utcnow().isoformat()
                        state["market_probe_reason"] = "rows_detected"
                        for c in _missing_codes_for_date(conn, "daily_price", probe_date):
                            if c and c not in seen:
                                seen.add(c)
                                targets.append(c)
                    else:
                        attempts = int(state.get("market_probe_attempts", 0) or 0)
                        retry_limit = int(cfg.get("market_probe_retry_limit", 6))
                        if bool(state.get("market_probe_holiday", False)):
                            state["market_probe_reason"] = "holiday_inferred"
                        elif attempts >= retry_limit:
                            state["market_probe_holiday"] = True
                            state["market_probe_holiday_at"] = datetime.utcnow().isoformat()
                            state["market_probe_reason"] = "no_rows_after_probe_attempts"
                        else:
                            sample_codes = _list_universe_codes(
                                conn,
                                limit=int(cfg.get("market_probe_sample_size", 20)),
                            )
                            for c in sample_codes:
                                if c and c not in seen:
                                    seen.add(c)
                                    targets.append(c)
                            probe_force_run = len(sample_codes) > 0
                            if probe_force_run:
                                probe_next_attempt = attempts + 1
                                state["market_probe_reason"] = "probe_pending"
                else:
                    state["market_probe_holiday"] = False
                    state["market_probe_reason"] = "up_to_date"

        msg = (
            f"[watchdog] date={last_date} "
            f"missing_latest={daily_missing_count} "
            f"missing_any={len(miss_any)} "
            f"invalid_latest={invalid_latest_count} "
            f"invalid_stale={invalid_stale_count}:{stale_start or 'N/A'} "
            f"targets={len(targets)} "
            f"probe_date={probe_date or '-'} "
            f"probe_rows={probe_codes_on_date} "
            f"probe_attempts={int(state.get('market_probe_attempts', 0) or 0)} "
            f"probe_holiday={int(bool(state.get('market_probe_holiday', False)))}"
        )
        maybe_notify(settings, msg)

        # Persist the latest observed health snapshot even when no refill is triggered.
        state["last_check_ts"] = time.time()
        state["last_check_at"] = datetime.utcnow().isoformat()
        state["last_check_date"] = last_date
        state["missing_latest"] = daily_missing_count
        state["missing_any"] = len(miss_any)
        state["invalid_latest"] = invalid_latest_count
        state["invalid_stale"] = invalid_stale_count
        if stale_start is not None:
            state["invalid_stale_start"] = stale_start
        else:
            state["invalid_stale_start"] = None
        state["targets"] = len(targets)
        _write_state(state_path, state)

        if cfg["no_refill"]:
            return

        # Daily refill (fill missing daily_price)
        if cfg["daily_enabled"] and (
            len(targets) >= int(cfg["daily_min_missing"])
            or len(miss_any) > 0
            or invalid_stale_count > 0
            or probe_force_run
        ):
            refill_lock_path: Path = cfg["refill_lock_file"]
            if _lock_active(refill_lock_path):
                logging.info("Refill lock active (%s); skipping daily refill.", refill_lock_path)
                return
            daily_lock_path: Path = cfg["daily_lock_file"]
            if _lock_active(daily_lock_path):
                maybe_notify(settings, "[watchdog] daily loader already running; skip")
            else:
                last_daily_ts = float(state.get("last_daily_run_ts", 0) or 0)
                cooldown = float(cfg["daily_cooldown"])
                now = time.time()
                if cooldown > 0 and (now - last_daily_ts) < cooldown:
                    remain = int(cooldown - (now - last_daily_ts))
                    maybe_notify(settings, f"[watchdog] daily cooldown active; skip ({remain}s left)")
                else:
                    daily_lock_path.parent.mkdir(parents=True, exist_ok=True)
                    daily_lock_path.write_text(str(os.getpid()), encoding="utf-8")
                    try:
                        if probe_force_run and probe_next_attempt is not None:
                            state["market_probe_attempts"] = int(probe_next_attempt)
                            state["market_probe_last_attempt_at"] = datetime.utcnow().isoformat()
                            state["market_probe_reason"] = "probe_run"
                        codes_file: Path = cfg["codes_file"]
                        _write_codes_csv(codes_file, targets)
                        maybe_notify(
                            settings,
                            (
                                f"[watchdog] daily refill start targets={len(targets)} "
                                f"date={last_date} stale_start={stale_start} "
                                f"probe_date={probe_date or '-'} probe_force={int(probe_force_run)}"
                            ),
                        )
                        rc = _run_daily_refill(
                            int(cfg["daily_chunk_days"]),
                            cfg["daily_limit"],
                            codes_file=str(codes_file),
                            include_latest=invalid_stale_count > 0,
                            include_latest_days=(
                                max(1, int(cfg.get("daily_stale_days", 1)))
                                if invalid_stale_count > 0
                                else 0
                            ),
                        )
                        state["last_daily_run_ts"] = time.time()
                        state["last_daily_date"] = last_date
                        state["last_daily_missing"] = daily_missing_count
                        state["last_daily_missing_any"] = len(miss_any)
                        state["last_daily_targets"] = len(targets)
                        state["last_daily_rc"] = rc
                        if cfg["daily_limit"] is not None:
                            state["last_daily_limit"] = int(cfg["daily_limit"])
                        _write_state(state_path, state)
                        if rc != 0:
                            maybe_notify(settings, f"[watchdog] daily refill exited rc={rc}")
                    finally:
                        try:
                            daily_lock_path.unlink()
                        except Exception:
                            pass

        # accuracy refill is intentionally disabled in trader-US.
        # (The legacy "accuracy_*" tables are KR-only and do not apply to US tickers.)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=None, help="loop interval seconds")
    parser.add_argument("--once", action="store_true", help="run once and exit")
    parser.add_argument("--no-refill", action="store_true", help="only notify, do not trigger refill")
    parser.add_argument("--cooldown", type=float, default=None, help="accuracy refill cooldown seconds")
    parser.add_argument("--daily-cooldown", type=float, default=None, help="daily refill cooldown seconds")
    parser.add_argument("--min-missing", type=int, default=None, help="min missing codes to trigger refill")
    parser.add_argument("--daily-min-missing", type=int, default=None, help="min missing daily codes to trigger refill")
    parser.add_argument("--daily-chunk-days", type=int, default=None, help="daily loader chunk days")
    parser.add_argument("--daily-stale-days", type=int, default=None, help="when invalid latest exists, refresh this many recent days ending at latest")
    parser.add_argument("--daily-limit", type=int, default=None, help="daily loader max code count per run")
    parser.add_argument("--daily-lock-file", type=str, default=None, help="daily loader lock file")
    parser.add_argument("--refill-lock-file", type=str, default=None, help="refill loader lock file (skip daily refill when active)")
    parser.add_argument("--notify-every", type=int, default=None, help="notify every n codes during refill")
    parser.add_argument("--item-sleep", type=float, default=None, help="sleep seconds per code during refill")
    parser.add_argument("--rate-sleep", type=float, default=None, help="override broker rate sleep for refill")
    parser.add_argument("--progress-file", type=str, default=None, help="progress file for watchdog refill")
    parser.add_argument("--codes-file", type=str, default=None, help="missing codes csv output")
    parser.add_argument("--accuracy-lock-file", type=str, default=None, help="accuracy loader lock file")
    parser.add_argument("--lock-file", type=str, default=None, help="watchdog lock file")
    parser.add_argument("--state-file", type=str, default=None, help="watchdog state json file")
    parser.add_argument("--limit", type=int, default=None, help="limit codes for testing")
    parser.add_argument("--daily-enabled", dest="daily_enabled", action="store_true", help="enable daily refill")
    parser.add_argument("--no-daily", dest="daily_enabled", action="store_false", help="disable daily refill")
    parser.set_defaults(daily_enabled=None)
    args = parser.parse_args()

    ensure_repo_root(Path(__file__).resolve())
    settings = load_settings()
    cfg = _load_cfg(settings, args)

    watchdog_lock = cfg["lock_file"]
    if _lock_active(watchdog_lock):
        maybe_notify(settings, "[watchdog] already running; exit")
        return
    watchdog_lock.parent.mkdir(parents=True, exist_ok=True)
    watchdog_lock.write_text(str(os.getpid()), encoding="utf-8")

    try:
        while True:
            run_once(settings, cfg)
            if cfg["once"]:
                break
            time.sleep(float(cfg["interval"]))
    finally:
        try:
            watchdog_lock.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    main()
