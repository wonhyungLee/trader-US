from __future__ import annotations

import os
import sqlite3
import logging
import json
import time
import threading
import subprocess
import sys
import hmac
import hashlib
import random
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, List

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

from src.analyzer.backtest_runner import load_strategy
from src.storage.sqlite_store import SQLiteStore, normalize_code
from src.utils.config import load_settings, list_kis_key_inventory, set_kis_key_enabled
from src.utils.db_exporter import maybe_export_db
from src.utils.project_root import ensure_repo_root
from src.autotrade.engine_adapter import recommend_daytrade_plan

ensure_repo_root(Path(__file__).resolve().parent)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SETTINGS = load_settings()
WATCHDOG_CFG = SETTINGS.get("watchdog", {}) or {}
DB_PATH = Path(SETTINGS.get("database", {}).get("path", "data/market_data.db"))
FRONTEND_DIST = Path("frontend/dist")
CLIENT_ERROR_LOG = Path("logs/client_error.log")
ACCOUNT_SNAPSHOT_PATH = Path("data/account_snapshot.json")
WATCHDOG_STATE_PATH = Path(WATCHDOG_CFG.get("state_file", "data/watchdog_state.json"))
WATCHDOG_DAILY_LOCK_PATH = Path(WATCHDOG_CFG.get("daily_lock_file", "data/daily_loader.lock"))
WATCHDOG_DAILY_CODES_FILE = Path(WATCHDOG_CFG.get("daily_codes_file", "data/csv/watchdog_daily_codes.csv"))
# Admin password for endpoints that mutate server state (filter toggles, sector overrides, etc).
# Do NOT hardcode secrets in git; configure via environment or a local .env (ignored).
KIS_TOGGLE_PASSWORD = os.getenv("KIS_TOGGLE_PASSWORD", "").strip()
AUTOTRADE_API_PASSWORD = os.getenv("AUTOTRADE_API_PASSWORD", "").strip()
FILTER_TOGGLE_PATH = Path("data/selection_filter_toggles.json")
FILTER_TOGGLE_KEYS = ("min_amount", "liquidity", "disparity")

_store = SQLiteStore(str(DB_PATH))
_store.conn.close()

AUTOTRADE_CFG = SETTINGS.get("autotrade", {}) or {}
LIST_SELECTED = "SELECTED"
LIST_EXIT = "EXIT"

_balance_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
_selection_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
_selection_lock = threading.Lock()
SELECTION_CACHE_TTL = float(os.getenv("SELECTION_CACHE_TTL", "60"))
SELECTION_CHANGE_LOG_DAYS = int(os.getenv("SELECTION_CHANGE_LOG_DAYS", "5"))
SELECTION_CHANGE_LOG_MAX_DAYS = int(os.getenv("SELECTION_CHANGE_LOG_MAX_DAYS", "20"))
STATUS_CACHE_TTL = float(os.getenv("STATUS_CACHE_TTL", "15"))
STATUS_HEAVY_INTERVAL_SEC = float(os.getenv("STATUS_HEAVY_INTERVAL_SEC", "300"))
_status_cache_lock = threading.Lock()
_status_cache: Dict[str, Any] = {"ts": 0.0, "heavy_ts": 0.0, "data": None}
CURRENT_PRICE_CACHE_TTL_SEC = float(os.getenv("CURRENT_PRICE_CACHE_TTL_SEC", "55"))
_current_price_cache: Dict[str, Dict[str, Any]] = {}
_current_price_lock = threading.Lock()

_coupang_banner_cache: Dict[str, Any] = {"ts": 0.0, "payload": None}
COUPANG_BANNER_CACHE_TTL_SEC = float(os.getenv("COUPANG_BANNER_CACHE_TTL_SEC", "1800"))
COUPANG_INFO_PATHS = [
    Path(os.getenv("COUPANG_INFO_PATH", "")).expanduser() if os.getenv("COUPANG_INFO_PATH") else None,
    Path("/home/ubuntu/쿠팡파트너스api정보.txt"),
    Path("/home/ubuntu/쿠팡파트너스 api정보.txt"),
    Path("쿠팡파트너스api정보.txt"),
    Path("쿠팡파트너스 api정보.txt"),
]

_watchdog_enabled_default = bool(WATCHDOG_CFG.get("enabled", True))
DB_WATCHDOG_ENABLED = os.getenv("BNF_DB_WATCHDOG_ENABLED", str(int(_watchdog_enabled_default))).strip().lower() not in {"0", "false", "no"}
DB_WATCHDOG_INTERVAL_SEC = float(os.getenv("BNF_DB_WATCHDOG_INTERVAL_SEC", str(WATCHDOG_CFG.get("interval_sec", 600))))
DB_WATCHDOG_DAILY_STALE_DAYS = int(os.getenv("BNF_DB_WATCHDOG_DAILY_STALE_DAYS", str(WATCHDOG_CFG.get("daily_stale_days", 1))))
DB_WATCHDOG_DAILY_CHUNK_DAYS = int(os.getenv("BNF_DB_WATCHDOG_DAILY_CHUNK_DAYS", str(WATCHDOG_CFG.get("daily_chunk_days", 90))))
DB_WATCHDOG_DAILY_COOLDOWN_SEC = float(os.getenv("BNF_DB_WATCHDOG_DAILY_COOLDOWN_SEC", str(WATCHDOG_CFG.get("daily_cooldown_sec", 3600))))
_invalid_latest_enabled_raw = os.getenv("BNF_DB_WATCHDOG_INVALID_LATEST_ENABLED")
if _invalid_latest_enabled_raw is None:
    DB_WATCHDOG_INVALID_LATEST_ENABLED = bool(WATCHDOG_CFG.get("invalid_latest_enabled", True))
else:
    DB_WATCHDOG_INVALID_LATEST_ENABLED = _invalid_latest_enabled_raw.strip().lower() not in {"0", "false", "no"}
DB_WATCHDOG_INVALID_LATEST_AMOUNT_THRESHOLD = float(
    os.getenv(
        "BNF_DB_WATCHDOG_INVALID_LATEST_AMOUNT_THRESHOLD",
        str(WATCHDOG_CFG.get("invalid_latest_amount_threshold", 0)),
    )
)
DB_WATCHDOG_INVALID_LATEST_VOLUME_THRESHOLD = float(
    os.getenv(
        "BNF_DB_WATCHDOG_INVALID_LATEST_VOLUME_THRESHOLD",
        str(WATCHDOG_CFG.get("invalid_latest_volume_threshold", 0)),
    )
)
DB_WATCHDOG_REFILL_CHUNK_DAYS = int(os.getenv("BNF_DB_WATCHDOG_REFILL_CHUNK_DAYS", str(WATCHDOG_CFG.get("refill_chunk_days", 150))))
DB_WATCHDOG_REFILL_SLEEP_SEC = float(os.getenv("BNF_DB_WATCHDOG_REFILL_SLEEP_SEC", str(WATCHDOG_CFG.get("refill_sleep_sec", 0.1))))
DB_WATCHDOG_REFILL_MAX_CODES = int(os.getenv("BNF_DB_WATCHDOG_REFILL_MAX_CODES", str(WATCHDOG_CFG.get("refill_max_missing_per_cycle", 1))))
DB_WATCHDOG_REFILL_COOLDOWN_SEC = float(os.getenv("BNF_DB_WATCHDOG_REFILL_COOLDOWN_SEC", str(WATCHDOG_CFG.get("refill_cooldown_sec", 120))))
DB_WATCHDOG_RUN_TIMEOUT_SEC = int(os.getenv("BNF_DB_WATCHDOG_RUN_TIMEOUT_SEC", "5400"))
_watchdog_thread: Optional[threading.Thread] = None
_watchdog_state_lock = threading.Lock()
_watchdog_state: Dict[str, Any] = {
    "enabled": DB_WATCHDOG_ENABLED,
    "running": False,
    "last_cycle_at": None,
    "last_daily_rc": None,
    "last_refill_rc": None,
    "last_daily_pid": None,
    "last_refill_pid": None,
    "last_daily_ts": 0.0,
    "last_refill_ts": 0.0,
    "last_error": None,
    "last_stats": {},
}

SELECTION_SNAPSHOT_TABLE = "selection_snapshots"
SELECTION_SNAPSHOT_VERSION = 2
SELECTION_CHANGES_DISCLAIMER = (
    '매수 후보(Selection)는 "지금 기준 신규 진입 후보"입니다. 후보에서 사라지는 것은 자동 매도 신호가 아닐 수 있으니, '
    "아래의 이탈 사유(조건/랭킹)를 확인하세요."
)
SELECTION_CHANGES_NOTE = "이탈 사유는 해당 날짜 기준 전략 조건으로 판정했습니다."


def get_conn(timeout: float = 5.0, busy_timeout_ms: int = 5000) -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
    except Exception:
        pass
    return conn


def _count(conn: sqlite3.Connection, table_expr: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_expr}").fetchone()[0]
    except Exception:
        return 0


def _minmax(conn: sqlite3.Connection, table: str) -> dict:
    try:
        row = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
        return {"min": row[0], "max": row[1]}
    except Exception:
        return {"min": None, "max": None}


def _distinct_code_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0]
    except Exception:
        return 0


def _missing_codes(conn: sqlite3.Connection, table: str) -> int:
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM universe_members u
            LEFT JOIN (SELECT DISTINCT code FROM {table}) t
            ON u.code = t.code
            WHERE t.code IS NULL
            """
        ).fetchone()
        return row[0]
    except Exception:
        return 0


def _codes_missing_on_date(conn: sqlite3.Connection, table: str, date_str: str) -> List[str]:
    try:
        rows = conn.execute(
            f"""
            SELECT u.code
            FROM universe_members u
            LEFT JOIN {table} t
              ON u.code = t.code
             AND t.date = ?
            WHERE t.code IS NULL
            ORDER BY u.code
            """,
            (date_str,),
        ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []


def _invalid_latest_codes(
    conn: sqlite3.Connection,
    amount_floor: float,
    volume_floor: float,
    date_str: str,
) -> List[str]:
    if not DB_WATCHDOG_INVALID_LATEST_ENABLED:
        return []
    if date_str is None:
        return []
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
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []


def _load_filter_toggles(path: Path = FILTER_TOGGLE_PATH) -> Dict[str, bool]:
    defaults = {key: True for key in FILTER_TOGGLE_KEYS}
    if not path.exists():
        return defaults
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return defaults
    if not isinstance(payload, dict):
        return defaults
    out = defaults.copy()
    for key in FILTER_TOGGLE_KEYS:
        if key in payload:
            out[key] = bool(payload.get(key))
    return out


def _save_filter_toggles(toggles: Dict[str, bool], path: Path = FILTER_TOGGLE_PATH) -> Dict[str, bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {key: bool(toggles.get(key, True)) for key in FILTER_TOGGLE_KEYS}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _check_password(password: Optional[str]) -> bool:
    if not KIS_TOGGLE_PASSWORD:
        return False
    return bool(password) and password == KIS_TOGGLE_PASSWORD


def _check_autotrade_password(password: Optional[str]) -> bool:
    if AUTOTRADE_API_PASSWORD:
        return bool(password) and password == AUTOTRADE_API_PASSWORD
    return _check_password(password)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return None


def _pick_float(payload: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for key in keys:
        if key in payload:
            val = _safe_float(payload.get(key))
            if val is not None:
                return val
    return None


def _is_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    return text.startswith("${") and text.endswith("}")


def _kis_ready(settings: Dict[str, Any]) -> bool:
    kis_cfg = settings.get("kis", {}) or {}
    app_key = kis_cfg.get("app_key")
    app_secret = kis_cfg.get("app_secret")
    if not app_key or not app_secret:
        return False
    if _is_placeholder(app_key) or _is_placeholder(app_secret):
        return False
    return True


def _extract_value_after_label(lines: List[str], labels: List[str]) -> Optional[str]:
    lowered = [l.strip().lower() for l in labels if l and str(l).strip()]
    for i, raw in enumerate(lines):
        line = str(raw or "").strip()
        if not line:
            continue
        low = line.lower()
        if any(low == lab or low.startswith(lab) for lab in lowered):
            for j in range(i + 1, min(len(lines), i + 10)):
                candidate = str(lines[j] or "").strip()
                if candidate:
                    return candidate
    return None


def _load_coupang_credentials() -> Optional[Dict[str, str]]:
    access = os.getenv("COUPANG_ACCESS_KEY", "").strip()
    secret = os.getenv("COUPANG_SECRET_KEY", "").strip()
    partner_id = os.getenv("COUPANG_PARTNER_ID", "").strip()
    sub_id = os.getenv("COUPANG_SUB_ID", "").strip() or "trader-us-banner"
    if access and secret:
        out = {"access_key": access, "secret_key": secret, "sub_id": sub_id}
        if partner_id:
            out["partner_id"] = partner_id
        return out

    for path in COUPANG_INFO_PATHS:
        if not path:
            continue
        try:
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        lines = text.splitlines()
        access_key = _extract_value_after_label(lines, ["access key", "access_key", "access-key"])
        secret_key = _extract_value_after_label(lines, ["secret key", "secret_key", "secret-key"])
        parsed_partner_id = _extract_value_after_label(lines, ["id", "partner id", "partner_id"])
        if access_key and secret_key:
            out = {"access_key": access_key, "secret_key": secret_key, "sub_id": sub_id}
            if parsed_partner_id and parsed_partner_id.upper().startswith("AF"):
                out["partner_id"] = parsed_partner_id
            return out

    return None


def _coupang_signed_date(now: Optional[datetime] = None) -> str:
    dt = now or datetime.utcnow()
    return dt.strftime("%y%m%dT%H%M%SZ")


def _coupang_hmac_signature(secret_key: str, message: str) -> str:
    digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest


def _fetch_coupang_search_products_with_keys(access_key: str, secret_key: str, keyword: str, limit: int, sub_id: str) -> List[Dict[str, Any]]:
    access_key = str(access_key or "").strip()
    secret_key = str(secret_key or "").strip()
    if not access_key or not secret_key:
        raise RuntimeError("coupang_credentials_missing")

    path = "/v2/providers/affiliate_open_api/apis/openapi/v1/products/search"
    # Keep query ordering stable for signature correctness.
    keyword_enc = requests.utils.quote(str(keyword), safe="")
    query = f"keyword={keyword_enc}&limit={int(limit)}&subId={requests.utils.quote(str(sub_id), safe='')}"

    signed_date = _coupang_signed_date()
    message = f"{signed_date}GET{path}{query}"
    signature = _coupang_hmac_signature(secret_key, message)
    authorization = (
        "CEA algorithm=HmacSHA256, "
        f"access-key={access_key}, "
        f"signed-date={signed_date}, "
        f"signature={signature}"
    )

    url = f"https://api-gateway.coupang.com{path}?{query}"
    resp = requests.get(url, headers={"Authorization": authorization}, timeout=10)
    if not resp.ok:
        raise RuntimeError(f"coupang_api_error status={resp.status_code}")
    data = resp.json() if resp.content else {}
    if isinstance(data, dict):
        rcode = str(data.get("rCode") or "")
        if rcode and rcode != "0":
            raise RuntimeError(f"coupang_api_error rCode={rcode}")
        payload = data.get("data") or {}
        products = payload.get("productData") or []
        if isinstance(products, list):
            return [p for p in products if isinstance(p, dict)]
    return []


def _fetch_coupang_search_products(keyword: str, limit: int, sub_id: str) -> List[Dict[str, Any]]:
    creds = _load_coupang_credentials()
    if not creds:
        raise RuntimeError("coupang_credentials_missing")
    return _fetch_coupang_search_products_with_keys(
        access_key=creds["access_key"],
        secret_key=creds["secret_key"],
        keyword=keyword,
        limit=limit,
        sub_id=sub_id,
    )


def _format_price_krw(value: Any) -> str:
    try:
        num = int(float(str(value).replace(",", "").strip()))
        return f"{num:,}원"
    except Exception:
        return ""


def _latest_price_map(conn: sqlite3.Connection, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    if not codes:
        return {}
    placeholder = ",".join("?" * len(codes))
    sql = f"""
        SELECT d.code, d.close, d.date
        FROM daily_price d
        JOIN (
            SELECT code, MAX(date) AS max_date
            FROM daily_price
            WHERE code IN ({placeholder})
            GROUP BY code
        ) m
        ON d.code = m.code AND d.date = m.max_date
    """
    rows = conn.execute(sql, tuple(codes)).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        out[row[0]] = {"close": row[1], "date": row[2]}
    return out


def _latest_price_row(code: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT date, close FROM daily_price WHERE code=? ORDER BY date DESC LIMIT 1",
            (code,),
        ).fetchone()
        if not row:
            return None
        return {"date": row[0], "close": row[1]}
    finally:
        conn.close()


def _fetch_yahoo_current_price(code: str) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper().replace(".", "-")
    if not symbol:
        raise ValueError("empty symbol")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    resp = requests.get(
        url,
        params={"range": "1d", "interval": "1m"},
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept": "application/json",
        },
        timeout=(4, 8),
    )
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    chart = payload.get("chart") or {}
    result_list = chart.get("result") or []
    if not result_list:
        raise RuntimeError(f"no result for symbol={symbol}")
    result = result_list[0] if isinstance(result_list, list) else result_list
    meta = result.get("meta") or {}
    indicators = (result.get("indicators") or {}).get("quote") or [{}]
    quote = indicators[0] if isinstance(indicators, list) and indicators else {}
    closes = quote.get("close") or []

    price = None
    for value in reversed(closes):
        if value is None:
            continue
        price = _safe_float(value)
        if price is not None:
            break
    if price is None:
        price = _safe_float(meta.get("regularMarketPrice"))

    prev_close = _safe_float(
        meta.get("regularMarketPreviousClose")
        or meta.get("previousClose")
        or meta.get("chartPreviousClose")
    )
    change = (price - prev_close) if (price is not None and prev_close is not None) else None
    change_pct = (change / prev_close * 100) if (change is not None and prev_close) else None

    market_time = meta.get("regularMarketTime")
    if market_time:
        asof = datetime.utcfromtimestamp(int(market_time)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        asof = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "code": symbol,
        "price": price,
        "prev_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "currency": meta.get("currency"),
        "exchange": meta.get("exchangeName"),
        "asof": asof,
        "source": "yahoo",
    }


def _fetch_stooq_current_price(code: str) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper()
    if not symbol:
        raise ValueError("empty symbol")
    stooq_symbol = f"{symbol}.US".lower()
    resp = requests.get(
        "https://stooq.com/q/l/",
        params={"s": stooq_symbol, "i": "1"},
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            "Accept": "text/plain",
        },
        timeout=(4, 8),
    )
    resp.raise_for_status()
    raw = (resp.text or "").strip()
    if not raw or raw.upper().startswith("N/D"):
        raise RuntimeError(f"stooq no data for {symbol}")

    # format: SYMBOL,YYYYMMDD,HHMMSS,OPEN,HIGH,LOW,CLOSE,VOLUME,...
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 7:
        raise RuntimeError(f"unexpected stooq format: {raw[:80]}")

    price = _safe_float(parts[6])
    d = parts[1] if len(parts) > 1 else ""
    t = parts[2] if len(parts) > 2 else ""
    asof = None
    if len(d) == 8 and len(t) == 6 and d.isdigit() and t.isdigit():
        try:
            asof = datetime.strptime(d + t, "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            asof = None
    if not asof:
        asof = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "code": symbol,
        "price": price,
        "asof": asof,
        "source": "stooq",
    }


def _load_account_snapshot() -> Optional[Dict[str, Any]]:
    if not ACCOUNT_SNAPSHOT_PATH.exists():
        return None
    try:
        return json.loads(ACCOUNT_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_account_snapshot(total_assets: Optional[float]) -> Optional[Dict[str, Any]]:
    if total_assets is None:
        return None
    if ACCOUNT_SNAPSHOT_PATH.exists():
        return _load_account_snapshot()
    snapshot = {
        "connected_at": pd.Timestamp.utcnow().isoformat(),
        "initial_total": total_assets,
    }
    try:
        ACCOUNT_SNAPSHOT_PATH.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return snapshot


def _fetch_live_balance(settings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        from src.brokers.kis_broker import KISBroker
    except Exception:
        return None
    try:
        broker = KISBroker(settings)
        if not hasattr(broker, "get_balance"):
            return None
        return broker.get_balance()
    except Exception:
        return None


def _build_account_summary(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Dict[str, Any]:
    now_ts = time.time()
    if _balance_cache.get("data") and now_ts - _balance_cache.get("ts", 0) < 120:
        return _balance_cache["data"]

    resp = _fetch_live_balance(settings)
    if not resp:
        data = {"connected": False, "reason": "balance_unavailable"}
        _balance_cache.update({"ts": now_ts, "data": data})
        return data

    output2 = resp.get("output2") or resp.get("output") or []
    summary = output2[0] if isinstance(output2, list) and output2 else (output2 if isinstance(output2, dict) else {})
    cash = _pick_float(summary, ("prcs_bal", "dnca_tot_amt", "cash_bal", "cash_bal_amt"))
    total_eval = _pick_float(summary, ("tot_evlu_amt", "tot_asst_evlu_amt"))
    total_pnl = _pick_float(summary, ("tot_pfls", "tot_pfls_amt"))

    positions = resp.get("output1") or []
    codes = []
    parsed_positions = []
    for p in positions:
        code = p.get("pdno") or p.get("PDNO")
        if not code:
            continue
        codes.append(code)
        parsed_positions.append({
            "code": code,
            "name": p.get("prdt_name") or p.get("PRDT_NAME") or "",
            "qty": int(float(p.get("hldg_qty") or p.get("HLDG_QTY") or 0)),
            "avg_price": _safe_float(p.get("pchs_avg_pric") or p.get("PCHS_AVG_PRIC")),
            "eval_amount": _safe_float(p.get("evlu_amt") or p.get("EVLU_AMT")),
        })

    price_map = _latest_price_map(conn, list(set(codes)))
    positions_value = 0.0
    for p in parsed_positions:
        if p["eval_amount"] is not None:
            positions_value += p["eval_amount"]
            continue
        last_close = price_map.get(p["code"], {}).get("close")
        if last_close is not None:
            positions_value += last_close * (p["qty"] or 0)

    if total_eval is None:
        total_eval = (cash or 0.0) + positions_value
    if total_pnl is None and total_eval is not None:
        cost = sum((p.get("avg_price") or 0) * (p.get("qty") or 0) for p in parsed_positions)
        total_pnl = total_eval - cost if cost else None

    snapshot = _save_account_snapshot(total_eval)
    since_pnl = None
    since_pct = None
    connected_at = None
    if snapshot and total_eval is not None:
        connected_at = snapshot.get("connected_at")
        initial_total = snapshot.get("initial_total") or 0
        since_pnl = total_eval - initial_total
        since_pct = (since_pnl / initial_total * 100) if initial_total else None

    data = {
        "connected": True,
        "connected_at": connected_at,
        "summary": {
            "cash": cash,
            "positions_value": positions_value,
            "total_assets": total_eval,
            "total_pnl": total_pnl,
            "total_pnl_pct": (total_pnl / total_eval * 100) if total_pnl is not None and total_eval else None,
        },
        "since_connected": {
            "pnl": since_pnl,
            "pnl_pct": since_pct,
        },
    }
    _balance_cache.update({"ts": now_ts, "data": data})
    return data


def _set_watchdog_state(**kwargs: Any) -> None:
    with _watchdog_state_lock:
        _watchdog_state.update(kwargs)


def _watchdog_snapshot() -> Dict[str, Any]:
    with _watchdog_state_lock:
        return dict(_watchdog_state)


def _external_watchdog_state(path: Path = WATCHDOG_STATE_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _lock_file_active(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        pid = int((path.read_text(encoding="utf-8") or "0").strip())
    except Exception:
        pid = 0
    if _pid_alive(pid):
        return True
    try:
        path.unlink()
    except Exception:
        pass
    return False


def _run_module(module: str, args: Optional[List[str]] = None, log_name: str = "watchdog.log") -> Tuple[int, Optional[int]]:
    cmd = [sys.executable, "-m", module] + (args or [])
    logging.info("[watchdog] run: %s", " ".join(cmd))
    try:
        log_path = Path("logs") / log_name
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as logf:
            proc = subprocess.Popen(
                cmd,
                cwd=str(Path(__file__).resolve().parent),
                stdout=logf,
                stderr=logf,
            )
        return 0, int(proc.pid)
    except Exception as exc:
        logging.exception("[watchdog] run failed: %s", exc)
        return 1, None


def _collect_watchdog_stats() -> Dict[str, Any]:
    conn = get_conn()
    try:
        universe_total = _count(conn, "universe_members")
        missing_codes = _missing_codes(conn, "daily_price")
        mm = _minmax(conn, "daily_price")
        max_date = mm.get("max")
        latest_missing_codes: List[str] = []
        invalid_latest_codes: List[str] = []
        if max_date:
            latest_missing_codes = _codes_missing_on_date(conn, "daily_price", str(max_date))
            invalid_latest_codes = _invalid_latest_codes(
                conn,
                DB_WATCHDOG_INVALID_LATEST_AMOUNT_THRESHOLD,
                DB_WATCHDOG_INVALID_LATEST_VOLUME_THRESHOLD,
                str(max_date),
            )
        stale_days = None
        if max_date:
            try:
                stale_days = (date.today() - datetime.strptime(str(max_date), "%Y-%m-%d").date()).days
            except Exception:
                stale_days = None
        return {
            "universe_total": universe_total,
            "missing_codes": missing_codes,
            "latest_missing_codes": latest_missing_codes,
            "max_date": max_date,
            "stale_days": stale_days,
            "invalid_latest_count": len(invalid_latest_codes),
            "invalid_latest_codes": invalid_latest_codes,
        }
    finally:
        conn.close()


def _missing_daily_codes(limit: int = 0) -> List[str]:
    conn = get_conn()
    try:
        sql = """
            SELECT u.code
            FROM universe_members u
            LEFT JOIN (SELECT DISTINCT code FROM daily_price) d
            ON u.code = d.code
            WHERE d.code IS NULL
            ORDER BY u.code
        """
        if limit and limit > 0:
            sql += " LIMIT ?"
            rows = conn.execute(sql, (limit,)).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    finally:
        conn.close()


def _run_refill_for_code(code: str) -> Tuple[int, Optional[int]]:
    return _run_module(
        "src.collectors.refill_loader",
        [
            "--code",
            str(code),
            "--chunk-days",
            str(DB_WATCHDOG_REFILL_CHUNK_DAYS),
            "--start-mode",
            "listing",
            "--sleep",
            str(DB_WATCHDOG_REFILL_SLEEP_SEC),
            "--resume",
        ],
        log_name="watchdog_refill.log",
    )


def _run_daily_loader(codes: Optional[List[str]] = None) -> Tuple[int, Optional[int]]:
    args = ["--chunk-days", str(DB_WATCHDOG_DAILY_CHUNK_DAYS)]
    if codes:
        target_codes = sorted({str(code).strip().upper() for code in codes if str(code).strip()})
        if target_codes:
            WATCHDOG_DAILY_CODES_FILE.parent.mkdir(parents=True, exist_ok=True)
            with WATCHDOG_DAILY_CODES_FILE.open("w", encoding="utf-8") as f:
                f.write("code\n")
                for code in target_codes:
                    f.write(f"{code}\n")
            args.extend(["--codes-file", str(WATCHDOG_DAILY_CODES_FILE)])
            logging.info("[watchdog] running daily_loader for %s target codes", len(target_codes))
    return _run_module(
        "src.collectors.daily_loader",
        args,
        log_name="watchdog_daily.log",
    )


def _module_running(module_keyword: str) -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-af", module_keyword],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except Exception:
        return False

    if result.returncode != 0:
        return False
    my_pid = os.getpid()
    project_root = str(Path(__file__).resolve().parent)
    for line in (result.stdout or "").splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except Exception:
            continue
        cmd = parts[1] if len(parts) > 1 else ""
        if pid != my_pid:
            if project_root in cmd:
                return True
    return False


def _watchdog_cycle() -> None:
    settings = load_settings()
    stats = _collect_watchdog_stats()
    now_ts = time.time()
    snapshot = _watchdog_snapshot()
    _set_watchdog_state(last_stats=stats, last_cycle_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

    if not _kis_ready(settings):
        _set_watchdog_state(last_error="kis_credentials_missing")
        return

    stale_days = stats.get("stale_days")
    missing_codes = int(stats.get("missing_codes") or 0)
    latest_missing_codes = [str(c).strip().upper() for c in (stats.get("latest_missing_codes") or [])]
    invalid_latest_codes = [str(c).strip().upper() for c in (stats.get("invalid_latest_codes") or [])]
    invalid_latest_count = int(stats.get("invalid_latest_count") or 0)
    should_daily = missing_codes > 0
    if isinstance(stale_days, int) and stale_days >= DB_WATCHDOG_DAILY_STALE_DAYS:
        should_daily = True
    if invalid_latest_count > 0:
        should_daily = True

    daily_running = _module_running("src.collectors.daily_loader")
    refill_running = _module_running("src.collectors.refill_loader")

    if daily_running:
        _set_watchdog_state(last_error="daily_loader_running")
    elif refill_running:
        _set_watchdog_state(last_error="refill_loader_running")

    refill_rc = None
    refill_pid = None
    # Prefer broad daily sync first; skip per-code refill while daily sync is needed/running.
    if missing_codes > 0 and not should_daily and not daily_running and not refill_running:
        last_refill_ts = float(snapshot.get("last_refill_ts") or 0.0)
        if (now_ts - last_refill_ts) >= DB_WATCHDOG_REFILL_COOLDOWN_SEC:
            targets = _missing_daily_codes(limit=DB_WATCHDOG_REFILL_MAX_CODES)
            for code in targets:
                refill_rc, refill_pid = _run_refill_for_code(code)
                _set_watchdog_state(
                    last_refill_rc=refill_rc,
                    last_refill_pid=refill_pid,
                    last_refill_ts=time.time(),
                )
                if refill_rc != 0:
                    break

    daily_rc = None
    daily_pid = None
    if should_daily and not daily_running and not refill_running:
        last_daily_ts = float(snapshot.get("last_daily_ts") or 0.0)
        if (now_ts - last_daily_ts) >= DB_WATCHDOG_DAILY_COOLDOWN_SEC:
            target_codes = []
            seen = set()
            for code in latest_missing_codes + invalid_latest_codes:
                key = str(code).strip().upper()
                if not key or key in seen:
                    continue
                seen.add(key)
                target_codes.append(key)
            daily_rc, daily_pid = _run_daily_loader(target_codes or None)
            _set_watchdog_state(
                last_daily_rc=daily_rc,
                last_daily_pid=daily_pid,
                last_daily_ts=time.time(),
            )

    if (daily_rc == 0) or (refill_rc == 0):
        _selection_cache.update({"ts": 0.0, "data": None})

    if (daily_rc and daily_rc != 0) or (refill_rc and refill_rc != 0):
        _set_watchdog_state(last_error=f"daily_rc={daily_rc}, refill_rc={refill_rc}")
    else:
        _set_watchdog_state(last_error=None)


def _db_watchdog_loop() -> None:
    _set_watchdog_state(running=True)
    logging.info(
        "[watchdog] started (interval=%ss stale_days=%s refill_max=%s)",
        DB_WATCHDOG_INTERVAL_SEC,
        DB_WATCHDOG_DAILY_STALE_DAYS,
        DB_WATCHDOG_REFILL_MAX_CODES,
    )
    time.sleep(5)
    while True:
        started = time.time()
        try:
            _watchdog_cycle()
        except Exception as exc:
            logging.exception("[watchdog] cycle failed")
            _set_watchdog_state(last_error=str(exc), last_cycle_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
        elapsed = time.time() - started
        time.sleep(max(5.0, DB_WATCHDOG_INTERVAL_SEC - elapsed))


def start_background_workers() -> None:
    global _watchdog_thread
    if not DB_WATCHDOG_ENABLED:
        logging.info("[watchdog] disabled by BNF_DB_WATCHDOG_ENABLED")
        return
    if _watchdog_thread and _watchdog_thread.is_alive():
        return
    _watchdog_thread = threading.Thread(target=_db_watchdog_loop, name="db-watchdog", daemon=True)
    _watchdog_thread.start()


app = Flask(__name__, static_folder=str(FRONTEND_DIST), static_url_path="")

def _admin_enabled() -> bool:
    return bool(os.getenv("ADMIN_TOKEN", "").strip())


def _is_admin_request() -> bool:
    token = os.getenv("ADMIN_TOKEN", "").strip()
    if not token:
        return False
    provided = request.headers.get("X-Admin-Token") or request.args.get("token") or ""
    return str(provided).strip() == token


def _require_admin_or_404() -> None:
    # If admin token isn't configured, hide the endpoint entirely.
    if not _admin_enabled():
        abort(404)
    if not _is_admin_request():
        abort(404)


cors_origins = os.getenv("CORS_ORIGINS", "").strip()
if cors_origins:
    origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
    if origins:
        CORS(app, resources={r"/*": {"origins": origins}})

DAILY_NECESSITIES_KEYWORDS = [
    "화장지",
    "물티슈",
    "키친타올",
    "주방세제",
    "세탁세제",
    "섬유유연제",
    "샴푸",
    "린스",
    "바디워시",
    "치약",
    "칫솔",
    "비누",
    "생수",
    "라면",
    "즉석밥",
    "쓰레기봉투",
    "고무장갑",
    "주방장갑",
    "위생장갑",
    "손소독제",
]


@app.get("/api/coupang-banner")
def coupang_banner():
    """Return a small set of Coupang Partners products for the site banner."""
    keyword_override = str(request.args.get("keyword") or "").strip()
    limit_raw = request.args.get("limit")
    try:
        limit = int(limit_raw) if limit_raw is not None else 1
    except Exception:
        limit = 1
    limit = max(1, min(3, limit))

    now = time.time()
    if not keyword_override:
        cached_payload = _coupang_banner_cache.get("payload")
        cached_ts = float(_coupang_banner_cache.get("ts") or 0.0)
        if cached_payload and (now - cached_ts) < COUPANG_BANNER_CACHE_TTL_SEC:
            return jsonify(cached_payload)

    creds = _load_coupang_credentials()
    if not creds:
        return jsonify({
            "keyword": keyword_override,
            "theme": {"id": "necessities", "title": "생필품 추천", "tagline": "오늘 필요한 생활 필수템", "cta": "쿠팡에서 보기"},
            "items": [],
            "error": "credentials_missing",
        })

    sub_id = creds.get("sub_id") or "trader-us-banner"
    access_key = creds.get("access_key") or ""
    secret_key = creds.get("secret_key") or ""
    if keyword_override:
        keyword = keyword_override
    else:
        # Make it stable per cache TTL bucket to reduce API calls under traffic bursts.
        bucket = int(now // max(COUPANG_BANNER_CACHE_TTL_SEC, 1))
        rng = random.Random(bucket)
        keyword = rng.choice(DAILY_NECESSITIES_KEYWORDS)

    try:
        products = _fetch_coupang_search_products_with_keys(
            access_key=access_key,
            secret_key=secret_key,
            keyword=keyword,
            limit=limit,
            sub_id=sub_id,
        )
    except Exception as exc:
        logging.warning("[coupang] banner fetch failed: %s", exc)
        return jsonify({
            "keyword": keyword,
            "theme": {"id": "necessities", "title": "생필품 추천", "tagline": "오늘 필요한 생활 필수템", "cta": "쿠팡에서 보기"},
            "items": [],
            "error": "fetch_failed",
        })

    items: List[Dict[str, Any]] = []
    ctas = ["최저가 보기", "쿠팡에서 보기", "리뷰 보고 선택"]
    for idx, product in enumerate(products[:limit]):
        title = str(product.get("productName") or "").strip()
        image = str(product.get("productImage") or "").strip()
        link = str(product.get("productUrl") or "").strip()
        if not title or not link:
            continue

        discount = None
        try:
            rate = float(product.get("productDiscountRate") or 0)
            if rate > 0:
                discount = int(round(rate))
        except Exception:
            discount = None

        rocket = bool(
            product.get("rocketWow")
            or product.get("rocket")
            or str(product.get("rocketDeliveryType") or "").upper() == "ROCKET"
            or product.get("isRocket")
            or product.get("isRocketWow")
        )
        free_shipping = bool(product.get("isFreeShipping") or product.get("freeShipping"))
        shipping_tag = "로켓배송" if rocket else ("무료배송" if free_shipping else "")

        rating_count = None
        rating = None
        try:
            rating_count = int(product.get("ratingCount") or product.get("reviewCount") or 0) or None
        except Exception:
            rating_count = None
        try:
            rating = float(product.get("rating") or product.get("ratingAverage") or product.get("ratingScore") or 0) or None
        except Exception:
            rating = None

        meta_parts: List[str] = []
        if isinstance(rating, (int, float)) and rating and rating > 0:
            meta_parts.append(f"★{rating:.1f}")
        if rating_count:
            meta_parts.append(f"리뷰 {rating_count:,}개")
        if shipping_tag:
            meta_parts.append(shipping_tag)
        category_name = str(product.get("categoryName") or "").strip()
        if category_name:
            meta_parts.append(category_name)

        items.append({
            "title": title,
            "image": image,
            "link": link,
            "price": _format_price_krw(product.get("productPrice")),
            "meta": " · ".join([m for m in meta_parts if m]),
            "badge": "생활필수품",
            "discountRate": discount,
            "cta": ctas[idx % len(ctas)],
            "shippingTag": shipping_tag,
            "ratingCount": rating_count,
            "rating": rating,
        })

    payload = {
        "keyword": keyword,
        "theme": {"id": "necessities", "title": "생필품 추천", "tagline": "오늘 필요한 생활 필수템", "cta": "쿠팡에서 보기"},
        "items": items,
    }
    if not keyword_override:
        _coupang_banner_cache.update({"ts": now, "payload": payload})
    return jsonify(payload)


@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def serve_static(path: str):
    if (FRONTEND_DIST / path).exists():
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


@app.get("/universe")
def universe():
    """Universe list (NASDAQ100 + S&P500)."""
    conn = get_conn()
    sector = request.args.get("sector")
    if sector:
        sector = str(sector).strip()
        # Backward-compat: old UI used 'UNKNOWN' as the missing-sector label.
        if sector.upper() == "UNKNOWN":
            sector = "미분류"
        where = "COALESCE(s.sector_name, '미분류') = ?"
        params = (sector,)
    else:
        where = "1=1"
        params = ()

    try:
        df = pd.read_sql_query(
            f"""
            SELECT u.code, u.name, u.market, u.group_name as 'group',
                   COALESCE(s.sector_name, '미분류') AS sector_name,
                   s.industry_name
            FROM universe_members u
            LEFT JOIN sector_map s ON u.code = s.code
            WHERE {where}
            ORDER BY u.code
            """,
            conn,
            params=params,
        )
    except Exception:
        df = pd.read_sql_query(
            "SELECT code, name, market, group_name as 'group' FROM universe_members ORDER BY code",
            conn,
        )
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient="records"))


@app.get("/sectors")
def sectors():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT u.market,
                   COALESCE(s.sector_name, '미분류') AS sector_name,
                   COUNT(*) AS count
            FROM universe_members u
            LEFT JOIN sector_map s ON u.code = s.code
            GROUP BY u.market, COALESCE(s.sector_name, '미분류')
            ORDER BY u.market, count DESC, sector_name
            """,
            conn,
        )
    except Exception:
        df = pd.DataFrame([], columns=["market", "sector_name", "count"])
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient="records"))


@app.get("/prices")
def prices():
    code = request.args.get("code")
    days = int(request.args.get("days", 360))
    if not code:
        return jsonify([])

    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT date, open, high, low, close, volume, amount, ma25, disparity
        FROM daily_price
        WHERE code=?
        ORDER BY date DESC
        LIMIT ?
        """,
        conn,
        params=(code, days),
    )
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient="records"))


@app.get("/current_price")
def current_price():
    code = str(request.args.get("code") or "").strip().upper()
    if not code:
        return jsonify({"error": "code is required"}), 400

    now = time.time()
    with _current_price_lock:
        cached = _current_price_cache.get(code)
        if cached and (now - float(cached.get("ts") or 0.0)) < CURRENT_PRICE_CACHE_TTL_SEC:
            return jsonify(cached.get("data") or {})

    data: Dict[str, Any] = {}
    try:
        data = _fetch_stooq_current_price(code)
    except Exception as stooq_exc:
        try:
            data = _fetch_yahoo_current_price(code)
        except Exception as yahoo_exc:
            logging.warning("[current_price] quote fetch failed for %s: stooq=%s yahoo=%s", code, stooq_exc, yahoo_exc)
            data = {"code": code, "source": "db"}

    latest = _latest_price_row(code)
    if latest:
        data["db_close"] = latest.get("close")
        data["db_date"] = latest.get("date")
        db_close = _safe_float(latest.get("close"))
        current = _safe_float(data.get("price"))
        if db_close is not None and current is not None and data.get("change") is None:
            data["change"] = current - db_close
        if db_close and current is not None and data.get("change_pct") is None:
            data["change_pct"] = (current - db_close) / db_close * 100
        if data.get("price") is None:
            data["price"] = latest.get("close")
            data["asof"] = f"{latest.get('date')}T00:00:00Z"
            data["source"] = "db"

    if data.get("price") is None:
        return jsonify({"error": "price not available", "code": code}), 404

    with _current_price_lock:
        _current_price_cache[code] = {"ts": now, "data": data}
    return jsonify(data)


@app.get("/portfolio")
def portfolio():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT p.code, p.name, p.qty, p.avg_price, p.entry_date, p.updated_at,
                   u.market, s.sector_name, s.industry_name
            FROM position_state p
            LEFT JOIN universe_members u ON p.code = u.code
            LEFT JOIN sector_map s ON p.code = s.code
            ORDER BY p.updated_at DESC
            """,
            conn,
        )
    except Exception:
        return jsonify({"positions": [], "totals": {"positions_value": 0, "cost": 0, "pnl": None, "pnl_pct": None}})

    codes = df["code"].dropna().astype(str).unique().tolist() if not df.empty else []
    price_map = _latest_price_map(conn, codes)
    records = []
    total_value = 0.0
    total_cost = 0.0
    for row in df.to_dict(orient="records"):
        code = row.get("code")
        last = price_map.get(code, {})
        last_close = last.get("close")
        last_date = last.get("date")
        qty = float(row.get("qty") or 0)
        avg_price = float(row.get("avg_price") or 0)
        cost = qty * avg_price if qty and avg_price else None
        market_value = qty * last_close if qty and last_close is not None else None
        pnl = market_value - cost if market_value is not None and cost is not None else None
        pnl_pct = (pnl / cost * 100) if pnl is not None and cost else None
        if market_value is not None:
            total_value += market_value
        if cost is not None:
            total_cost += cost
        row.update(
            {
                "last_close": last_close,
                "last_date": last_date,
                "market_value": market_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
            }
        )
        records.append(row)

    totals = {
        "positions_value": total_value,
        "cost": total_cost,
        "pnl": total_value - total_cost if total_cost else None,
        "pnl_pct": ((total_value - total_cost) / total_cost * 100) if total_cost else None,
    }
    return jsonify({"positions": records, "totals": totals})


@app.get("/plans")
def plans():
    conn = get_conn()
    exec_date = request.args.get("exec_date")
    if not exec_date:
        try:
            exec_date = conn.execute("SELECT MAX(exec_date) FROM order_queue").fetchone()[0]
        except Exception:
            exec_date = None
    if not exec_date:
        return jsonify({"exec_date": None, "buys": [], "sells": []})

    try:
        df = pd.read_sql_query(
            """
            SELECT o.id, o.signal_date, o.exec_date, o.code, o.side, o.qty, o.rank, o.status,
                   o.ord_dvsn, o.ord_unpr, o.stop_unpr, o.target_unpr, o.strategy, o.meta_json,
                   o.created_at, o.updated_at,
                   u.name, u.market, s.sector_name, s.industry_name
            FROM order_queue o
            LEFT JOIN universe_members u ON o.code = u.code
            LEFT JOIN sector_map s ON o.code = s.code
            WHERE o.exec_date = ? AND o.status IN ('PENDING','SENT','PARTIAL','NOT_FOUND')
            ORDER BY o.rank ASC, o.id ASC
            """,
            conn,
            params=(exec_date,),
        )
    except Exception:
        return jsonify({"exec_date": exec_date, "buys": [], "sells": [], "counts": {"buys": 0, "sells": 0}})

    codes = df["code"].dropna().astype(str).unique().tolist() if not df.empty else []
    price_map = _latest_price_map(conn, codes)
    buys = []
    sells = []
    for row in df.to_dict(orient="records"):
        code = row.get("code")
        last = price_map.get(code, {})
        planned_price = row.get("ord_unpr") if row.get("ord_unpr") else last.get("close")
        row.update(
            {
                "planned_price": planned_price,
                "last_close": last.get("close"),
                "last_date": last.get("date"),
            }
        )
        if row.get("side") == "SELL":
            sells.append(row)
        else:
            buys.append(row)

    return jsonify(
        {
            "exec_date": exec_date,
            "buys": buys,
            "sells": sells,
            "counts": {"buys": len(buys), "sells": len(sells)},
        }
    )


@app.get("/account")
def account():
    conn = get_conn()
    settings = load_settings()
    return jsonify(_build_account_summary(conn, settings))


@app.get("/kis_keys")
def kis_keys():
    inventory = list_kis_key_inventory()
    # Normalize fields for UI compatibility
    enriched = []
    for item in inventory:
        row = dict(item)
        row["account"] = item.get("account_no_masked") or item.get("label")
        row.setdefault("env", "real")
        enriched.append(row)
    return jsonify(enriched)


@app.post("/kis_keys/toggle")
def kis_keys_toggle():
    payload = request.get_json(silent=True) or {}
    if not _check_password(payload.get("password")):
        return jsonify({"error": "invalid_password"}), 403
    try:
        idx = int(payload.get("id"))
    except Exception:
        return jsonify({"error": "invalid_id"}), 400
    if idx < 1 or idx > 50:
        return jsonify({"error": "invalid_id"}), 400
    enabled = bool(payload.get("enabled"))
    updated = set_kis_key_enabled(idx, enabled)
    # same shape as /kis_keys
    enriched = []
    for item in updated:
        row = dict(item)
        row["account"] = item.get("account_no_masked") or item.get("label")
        row.setdefault("env", "real")
        enriched.append(row)
    return jsonify(enriched)


def _selection_strategy_id(params: Any, toggles: Dict[str, bool]) -> str:
    """Stable identifier for selection snapshot grouping (strategy params + UI toggles)."""
    cfg = {
        "min_amount": float(getattr(params, "min_amount", 0) or 0),
        "liquidity_rank": int(getattr(params, "liquidity_rank", 0) or 0),
        # NOTE: kept for backward-compat with existing strategy param naming.
        "buy_nasdaq": float(getattr(params, "buy_kospi", 0) or 0),
        "buy_sp500": float(getattr(params, "buy_kosdaq", 0) or 0),
        "max_positions": int(getattr(params, "max_positions", 20) or 20),
        "max_per_sector": int(getattr(params, "max_per_sector", 0) or 0),
        "rank_mode": str(getattr(params, "rank_mode", "amount") or "amount").lower(),
        "entry_mode": str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower(),
        "trend_ma25_rising": bool(getattr(params, "trend_ma25_rising", False)),
        "toggles": {k: bool(toggles.get(k, True)) for k in FILTER_TOGGLE_KEYS},
    }
    blob = json.dumps(cfg, sort_keys=True, separators=(",", ":"))
    hid = hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]
    return f"sel-{hid}"


def _ensure_selection_snapshot_schema(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SELECTION_SNAPSHOT_TABLE} (
                asof_date TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (asof_date, strategy_id)
            )
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{SELECTION_SNAPSHOT_TABLE}_strategy_date
            ON {SELECTION_SNAPSHOT_TABLE}(strategy_id, asof_date)
            """
        )
        conn.commit()
    except Exception:
        logging.exception("failed to ensure %s schema", SELECTION_SNAPSHOT_TABLE)


def _selection_snapshot_exists(conn: sqlite3.Connection, asof_date: str, strategy_id: str) -> bool:
    try:
        row = conn.execute(
            f"SELECT 1 FROM {SELECTION_SNAPSHOT_TABLE} WHERE asof_date=? AND strategy_id=? LIMIT 1",
            (asof_date, strategy_id),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _store_selection_snapshot(conn: sqlite3.Connection, snapshot: Dict[str, Any]) -> None:
    asof_date = str(snapshot.get("asof_date") or "").strip()
    strategy_id = str(snapshot.get("strategy_id") or "").strip()
    if not asof_date or not strategy_id:
        return
    try:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {SELECTION_SNAPSHOT_TABLE} (asof_date, strategy_id, snapshot_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                asof_date,
                strategy_id,
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    except Exception:
        logging.exception("failed to store selection snapshot %s/%s", asof_date, strategy_id)


def _fetch_recent_trading_dates(conn: sqlite3.Connection, latest_date: str, limit: int) -> List[str]:
    if not latest_date:
        return []
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM daily_price
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (latest_date, int(limit)),
        ).fetchall()
        out: List[str] = []
        for r in rows:
            if not r:
                continue
            d = str(r[0]).strip()
            if d:
                out.append(d)
        return out
    except Exception:
        return []


def _compute_selection_snapshot_for_date(
    conn: sqlite3.Connection,
    params: Any,
    toggles: Dict[str, bool],
    asof_date: str,
    strategy_id: str,
) -> Dict[str, Any]:
    """Compute a point-in-time selection snapshot for change-log (asof_date 기준).

    Important: This is NOT a sell signal. Selection is "new entry candidates as-of-date".
    """
    asof_date = str(asof_date or "").strip()
    universe_df = pd.read_sql_query("SELECT code, name, market, group_name FROM universe_members", conn)
    universe_total = int(len(universe_df))
    codes = universe_df["code"].dropna().astype(str).tolist()
    if not codes or not asof_date:
        return {
            "version": SELECTION_SNAPSHOT_VERSION,
            "asof_date": asof_date,
            "strategy_id": strategy_id,
            "universe_total": universe_total,
            "selected": [],
            "eval": {},
            "note": SELECTION_CHANGES_NOTE,
        }

    min_amount = float(getattr(params, "min_amount", 0) or 0)
    liquidity_rank = int(getattr(params, "liquidity_rank", 0) or 0)
    buy_nasdaq = float(getattr(params, "buy_kospi", 0) or 0)
    buy_sp500 = float(getattr(params, "buy_kosdaq", 0) or 0)
    max_positions = int(getattr(params, "max_positions", 20) or 20)
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    trend_filter = bool(getattr(params, "trend_ma25_rising", False))

    rows: List[Tuple[Any, ...]] = []
    sql = """
        SELECT code, date, close, amount, ma25, disparity
        FROM daily_price
        WHERE code = ?
          AND date <= ?
        ORDER BY date DESC
        LIMIT 4
    """
    for code in codes:
        rows.extend(conn.execute(sql, (code, asof_date)).fetchall())

    df = pd.DataFrame(rows, columns=["code", "date", "close", "amount", "ma25", "disparity"])
    if df.empty:
        return {
            "version": SELECTION_SNAPSHOT_VERSION,
            "asof_date": asof_date,
            "strategy_id": strategy_id,
            "universe_total": universe_total,
            "selected": [],
            "eval": {},
            "note": SELECTION_CHANGES_NOTE,
        }

    df = df.sort_values(["code", "date"])
    df["ma25_prev"] = df.groupby("code")["ma25"].shift(1)
    df["close_prev"] = df.groupby("code")["close"].shift(1)
    df["close_delta_pct"] = df.groupby("code")["close"].pct_change(1) * 100
    df["ret3"] = df.groupby("code")["close"].pct_change(3)
    latest = df.groupby("code").tail(1).copy()
    latest = latest.merge(universe_df, on="code", how="left")
    try:
        sector_df = pd.read_sql_query("SELECT code, sector_name, industry_name FROM sector_map", conn)
        latest = latest.merge(sector_df, on="code", how="left")
    except Exception:
        pass

    stage_universe = latest

    stage_min_amount = stage_universe
    if min_amount and toggles.get("min_amount", True):
        stage_min_amount = stage_universe[stage_universe["amount"] >= min_amount]

    stage_liquidity = stage_min_amount
    if liquidity_rank and toggles.get("liquidity", True):
        stage_liquidity = stage_min_amount.sort_values("amount", ascending=False).head(liquidity_rank)

    def pass_signal(row) -> bool:
        group = str(row.get("group_name") or row.get("market") or "").upper()
        threshold = buy_nasdaq if "NASDAQ" in group else buy_sp500
        try:
            disp = float(row.get("disparity") or 0)
            r3 = float(row.get("ret3") or 0)
        except Exception:
            return False
        if entry_mode == "trend_follow":
            return disp >= threshold and r3 >= 0
        return disp <= threshold

    stage_disparity = stage_liquidity
    if toggles.get("disparity", True):
        stage_disparity = stage_liquidity[stage_liquidity.apply(pass_signal, axis=1)]

    stage_trend = stage_disparity
    if trend_filter:
        stage_trend = stage_disparity[stage_disparity["ma25_prev"].notna() & (stage_disparity["ma25"] > stage_disparity["ma25_prev"])]

    ranked = stage_trend.copy()
    if rank_mode == "score":
        if entry_mode == "trend_follow":
            ranked["score"] = (
                (ranked["disparity"].fillna(0).astype(float))
                + (0.8 * (ranked["ret3"].fillna(0).astype(float)))
                + (0.05 * np.log1p(ranked["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        else:
            ranked["score"] = (
                (-ranked["disparity"].fillna(0).astype(float))
                + (0.8 * (-ranked["ret3"].fillna(0).astype(float)))
                + (0.05 * np.log1p(ranked["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        ranked = ranked.sort_values("score", ascending=False)
    else:
        ranked = ranked.sort_values("amount", ascending=False)

    final_rows = []
    sector_counts: Dict[str, int] = {}
    try:
        held = conn.execute(
            """
            SELECT p.code,
                   COALESCE(s.sector_name, u.group_name, '미분류') AS sec
            FROM position_state p
            LEFT JOIN sector_map s ON p.code = s.code
            LEFT JOIN universe_members u ON p.code = u.code
            """
        ).fetchall()
        for code, sec in held:
            sec = sec or "미분류"
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}

    skipped_sector: set[str] = set()
    stop_pos: Optional[int] = None
    for pos, (_, row) in enumerate(ranked.iterrows()):
        sec = row.get("sector_name") or "미분류"
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        if max_per_sector and sector_counts.get(sec, 0) >= max_per_sector:
            skipped_sector.add(code)
            continue
        final_rows.append(row)
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(final_rows) >= max_positions:
            stop_pos = pos
            break

    final = pd.DataFrame(final_rows) if final_rows else ranked.head(0).copy()
    if not final.empty:
        final["rank"] = range(1, len(final) + 1)

    selected_items: List[Dict[str, Any]] = []
    if not final.empty:
        try:
            for _, row in final.iterrows():
                code = str(row.get("code") or "").strip().upper()
                if not code:
                    continue
                name = str(row.get("name") or "").strip() or code
                selected_items.append({"code": code, "name": name})
        except Exception:
            selected_items = []

    universe_codes = [str(c).strip().upper() for c in codes if c]
    universe_set = set(universe_codes)
    stage_universe_set = set(stage_universe["code"].dropna().astype(str).str.upper().tolist()) if not stage_universe.empty else set()
    stage_min_amount_set = set(stage_min_amount["code"].dropna().astype(str).str.upper().tolist()) if not stage_min_amount.empty else set()
    stage_liquidity_set = set(stage_liquidity["code"].dropna().astype(str).str.upper().tolist()) if not stage_liquidity.empty else set()
    stage_disparity_set = set(stage_disparity["code"].dropna().astype(str).str.upper().tolist()) if not stage_disparity.empty else set()
    stage_trend_set = set(stage_trend["code"].dropna().astype(str).str.upper().tolist()) if not stage_trend.empty else set()
    final_set = set(final["code"].dropna().astype(str).str.upper().tolist()) if not final.empty else set()

    # Name lookup fallback.
    name_map: Dict[str, str] = {}
    try:
        for _, r in universe_df.iterrows():
            code = str(r.get("code") or "").strip().upper()
            if not code:
                continue
            name = str(r.get("name") or "").strip()
            if name:
                name_map[code] = name
    except Exception:
        name_map = {}

    latest_by_code = {}
    try:
        if not latest.empty:
            latest_by_code = {str(r.get("code") or "").strip().upper(): r for _, r in latest.iterrows()}
    except Exception:
        latest_by_code = {}

    def _jfloat(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _reason_for(code: str) -> Tuple[str, str]:
        # Stage order mirrors the dashboard summary logic.
        if code not in stage_universe_set:
            return ("data_missing", "가격 데이터 부족")
        if code not in stage_min_amount_set:
            return ("min_amount", "거래대금 기준 미달")
        if code not in stage_liquidity_set:
            return ("liquidity", "거래대금 상위 순위 밖(유동성 필터)")
        if code not in stage_disparity_set:
            return ("disparity", "괴리율(및 모멘텀) 조건 미충족")
        if code not in stage_trend_set:
            if not trend_filter:
                return ("unknown", "이탈")
            row = latest_by_code.get(code) or {}
            try:
                prev = row.get("ma25_prev")
            except Exception:
                prev = None
            if prev is None or pd.isna(prev):
                return ("trend_ma25_missing", "상승추세(MA25) 데이터 부족")
            return ("trend_ma25", "상승추세(MA25) 조건 붕괴")
        if code not in final_set:
            if code in skipped_sector:
                return ("sector_cap", "섹터 제한")
            return ("final_count_cap", "최종 후보 수 제한")
        return ("selected", "매수 후보")

    eval_map: Dict[str, Any] = {}
    for code in sorted(universe_set):
        rc, rt = _reason_for(code)
        row = latest_by_code.get(code)
        close = close_prev = close_delta_pct = None
        if row is not None:
            try:
                close = _jfloat(row.get("close"))
                close_prev = _jfloat(row.get("close_prev"))
                close_delta_pct = _jfloat(row.get("close_delta_pct"))
            except Exception:
                close = close_prev = close_delta_pct = None
        eval_map[code] = {
            "code": code,
            "name": name_map.get(code) or (code),
            "selected": bool(code in final_set),
            "reason_code": rc,
            "reason_text": rt,
            "close": close,
            "close_prev": close_prev,
            "close_delta_pct": close_delta_pct,
        }

    return {
        "version": SELECTION_SNAPSHOT_VERSION,
        "asof_date": asof_date,
        "strategy_id": strategy_id,
        "universe_total": universe_total,
        "selected": selected_items,
        "eval": eval_map,
        "note": SELECTION_CHANGES_NOTE,
    }


def _build_selection_changes(
    conn: sqlite3.Connection,
    settings: Dict[str, Any],
    params: Any,
    toggles: Dict[str, bool],
    latest_date: Optional[str],
) -> Dict[str, Any]:
    """Return recent selection change-log (added/dropped) for UI."""
    try:
        days_cfg = int(settings.get("ui", {}).get("selection_change_days", SELECTION_CHANGE_LOG_DAYS))
    except Exception:
        days_cfg = SELECTION_CHANGE_LOG_DAYS
    days = max(1, min(int(days_cfg or SELECTION_CHANGE_LOG_DAYS), int(SELECTION_CHANGE_LOG_MAX_DAYS or 20)))
    if not latest_date:
        return {
            "days": days,
            "ready": False,
            "summary": {"added": 0, "dropped": 0},
            "added": [],
            "dropped": [],
            "disclaimer": SELECTION_CHANGES_DISCLAIMER,
            "note": SELECTION_CHANGES_NOTE,
        }

    strategy_id = _selection_strategy_id(params, toggles)
    _ensure_selection_snapshot_schema(conn)

    # Need baseline + window days to compute per-day changes.
    compare_dates_desc = _fetch_recent_trading_dates(conn, str(latest_date), days + 1)
    compare_dates = list(reversed(compare_dates_desc))  # oldest -> newest
    if len(compare_dates) < 2:
        return {
            "days": days,
            "ready": False,
            "strategy_id": strategy_id,
            "dates": [],
            "summary": {"added": 0, "dropped": 0},
            "added": [],
            "dropped": [],
            "disclaimer": SELECTION_CHANGES_DISCLAIMER,
            "note": SELECTION_CHANGES_NOTE,
        }

    # Backfill missing snapshots in this window only (fast after first run).
    for d in compare_dates:
        if _selection_snapshot_exists(conn, d, strategy_id):
            continue
        snap = _compute_selection_snapshot_for_date(conn, params, toggles, d, strategy_id)
        _store_selection_snapshot(conn, snap)

    snapshots: Dict[str, Dict[str, Any]] = {}
    for d in compare_dates:
        snap: Optional[Dict[str, Any]] = None
        try:
            row = conn.execute(
                f"SELECT snapshot_json FROM {SELECTION_SNAPSHOT_TABLE} WHERE asof_date=? AND strategy_id=?",
                (d, strategy_id),
            ).fetchone()
            if row and row[0]:
                parsed = json.loads(row[0])
                if isinstance(parsed, dict):
                    snap = parsed
        except Exception:
            snap = None

        if not snap or int(snap.get("version") or 0) != SELECTION_SNAPSHOT_VERSION:
            try:
                snap = _compute_selection_snapshot_for_date(conn, params, toggles, d, strategy_id)
                _store_selection_snapshot(conn, snap)
            except Exception:
                logging.exception("failed to rebuild snapshot %s/%s", d, strategy_id)
                snap = snap or {}

        if snap:
            snapshots[d] = snap

    added_events: List[Dict[str, Any]] = []
    dropped_events: List[Dict[str, Any]] = []

    for i in range(1, len(compare_dates)):
        d = compare_dates[i]
        prev = compare_dates[i - 1]
        cur = snapshots.get(d) or {}
        prev_snap = snapshots.get(prev) or {}

        cur_selected = {str(x.get("code") or "").strip().upper() for x in (cur.get("selected") or []) if x and x.get("code")}
        prev_selected = {str(x.get("code") or "").strip().upper() for x in (prev_snap.get("selected") or []) if x and x.get("code")}
        if not cur_selected and not prev_selected:
            continue

        cur_eval = cur.get("eval") or {}
        prev_eval = prev_snap.get("eval") or {}

        for code in sorted(cur_selected - prev_selected):
            ev = cur_eval.get(code) or prev_eval.get(code) or {}
            name = str(ev.get("name") or "").strip() or code
            added_events.append({"date": d, "code": code, "name": name})

        for code in sorted(prev_selected - cur_selected):
            ev = cur_eval.get(code) or prev_eval.get(code) or {}
            name = str(ev.get("name") or "").strip() or code
            reason_text = str(ev.get("reason_text") or "").strip() or "이탈"
            reason_code = str(ev.get("reason_code") or "").strip() or "unknown"
            dropped_events.append(
                {
                    "date": d,
                    "code": code,
                    "name": name,
                    "reason": reason_text,
                    "reason_code": reason_code,
                    "close_delta_pct": ev.get("close_delta_pct"),
                }
            )

    def _sort_key(ev: Dict[str, Any]) -> Tuple[str, str]:
        # date desc (string YYYY-MM-DD), code asc
        return (str(ev.get("date") or ""), str(ev.get("code") or ""))

    added_events = sorted(added_events, key=_sort_key, reverse=True)
    dropped_events = sorted(dropped_events, key=_sort_key, reverse=True)

    return {
        "days": days,
        "ready": True,
        "strategy_id": strategy_id,
        "dates": compare_dates[1:],  # window dates (exclude baseline)
        "summary": {"added": len(added_events), "dropped": len(dropped_events)},
        "added": added_events,
        "dropped": dropped_events,
        "disclaimer": SELECTION_CHANGES_DISCLAIMER,
        "note": SELECTION_CHANGES_NOTE,
    }


def _build_selection_summary(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Selection summary used by the dashboard (selection-only)."""
    params = load_strategy(settings)
    toggles = _load_filter_toggles()

    min_amount = float(getattr(params, "min_amount", 0) or 0)
    liquidity_rank = int(getattr(params, "liquidity_rank", 0) or 0)
    buy_nasdaq = float(getattr(params, "buy_kospi", 0) or 0)
    buy_sp500 = float(getattr(params, "buy_kosdaq", 0) or 0)
    max_positions = int(getattr(params, "max_positions", 20) or 20)
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    trend_filter = bool(getattr(params, "trend_ma25_rising", False))

    universe_df = pd.read_sql_query("SELECT code, name, market, group_name FROM universe_members", conn)
    universe_total = int(len(universe_df))
    codes = universe_df["code"].dropna().astype(str).tolist()
    if not codes:
        return {
            "date": None,
            "candidates": [],
            "stages": [],
            "pricing": {},
            "stage_items": {},
            "filter_toggles": toggles,
            "changes": _build_selection_changes(conn, settings, params, toggles, None),
        }

    rows: List[Tuple[Any, ...]] = []
    sql = """
        SELECT code, date, close, amount, ma25, disparity
        FROM daily_price
        WHERE code = ?
        ORDER BY date DESC
        LIMIT 4
    """
    for code in codes:
        rows.extend(conn.execute(sql, (code,)).fetchall())
    df = pd.DataFrame(rows, columns=["code", "date", "close", "amount", "ma25", "disparity"])
    if df.empty:
        return {
            "date": None,
            "candidates": [],
            "stages": [],
            "pricing": {},
            "stage_items": {},
            "filter_toggles": toggles,
            "changes": _build_selection_changes(conn, settings, params, toggles, None),
        }

    df = df.sort_values(["code", "date"])
    df["ma25_prev"] = df.groupby("code")["ma25"].shift(1)
    df["ret3"] = df.groupby("code")["close"].pct_change(3)
    latest = df.groupby("code").tail(1).copy()
    latest = latest.merge(universe_df, on="code", how="left")
    try:
        sector_df = pd.read_sql_query("SELECT code, sector_name, industry_name FROM sector_map", conn)
        latest = latest.merge(sector_df, on="code", how="left")
    except Exception:
        pass

    latest_date = latest["date"].max()

    stage_universe = latest

    stage_min_amount = stage_universe
    if min_amount and toggles.get("min_amount", True):
        stage_min_amount = stage_universe[stage_universe["amount"] >= min_amount]

    stage_liquidity = stage_min_amount
    if liquidity_rank and toggles.get("liquidity", True):
        stage_liquidity = stage_min_amount.sort_values("amount", ascending=False).head(liquidity_rank)

    def pass_signal(row) -> bool:
        group = str(row.get("group_name") or row.get("market") or "").upper()
        threshold = buy_nasdaq if "NASDAQ" in group else buy_sp500
        try:
            disp = float(row.get("disparity") or 0)
            r3 = float(row.get("ret3") or 0)
        except Exception:
            return False
        if entry_mode == "trend_follow":
            return disp >= threshold and r3 >= 0
        return disp <= threshold

    stage_disparity = stage_liquidity
    if toggles.get("disparity", True):
        stage_disparity = stage_liquidity[stage_liquidity.apply(pass_signal, axis=1)]
    if trend_filter:
        stage_disparity = stage_disparity[stage_disparity["ma25_prev"].notna() & (stage_disparity["ma25"] > stage_disparity["ma25_prev"])]

    ranked = stage_disparity.copy()
    if rank_mode == "score":
        if entry_mode == "trend_follow":
            ranked["score"] = (
                (ranked["disparity"].fillna(0).astype(float))
                + (0.8 * (ranked["ret3"].fillna(0).astype(float)))
                + (0.05 * np.log1p(ranked["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        else:
            ranked["score"] = (
                (-ranked["disparity"].fillna(0).astype(float))
                + (0.8 * (-ranked["ret3"].fillna(0).astype(float)))
                + (0.05 * np.log1p(ranked["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        ranked = ranked.sort_values("score", ascending=False)
    else:
        ranked = ranked.sort_values("amount", ascending=False)

    final_rows = []
    sector_counts: Dict[str, int] = {}
    try:
        held = conn.execute(
            """
            SELECT p.code,
                   COALESCE(s.sector_name, u.group_name, '미분류') AS sec
            FROM position_state p
            LEFT JOIN sector_map s ON p.code = s.code
            LEFT JOIN universe_members u ON p.code = u.code
            """
        ).fetchall()
        for code, sec in held:
            sec = sec or "미분류"
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}
    for _, row in ranked.iterrows():
        sec = row.get("sector_name") or "미분류"
        if max_per_sector and sector_counts.get(sec, 0) >= max_per_sector:
            continue
        final_rows.append(row)
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(final_rows) >= max_positions:
            break

    final = pd.DataFrame(final_rows) if final_rows else ranked.head(0).copy()
    if not final.empty:
        final["rank"] = range(1, len(final) + 1)

    daytrade_cfg = load_daytrade_cfg(settings)
    daytrade_enabled = bool(daytrade_cfg.get("enabled", False))
    signal_date = latest_price_date(conn)
    if not daytrade_enabled:
        default_plan_status = "daytrade_disabled"
    elif not signal_date:
        default_plan_status = "no_price_data"
    else:
        default_plan_status = "wait"

    def _empty_candidate_plan(status: str = default_plan_status) -> Dict[str, Any]:
        return {
            "status": status,
            "signal_date": str(signal_date) if signal_date else None,
            "entry_price": None,
            "target_price": None,
            "stop_price": None,
        }

    plan_by_code: Dict[str, Dict[str, Any]] = {}
    if not final.empty:
        for _, row in final.iterrows():
            code = str(row.get("code") or "").strip().upper()
            if not code:
                continue
            if not (daytrade_enabled and signal_date):
                plan_by_code[code] = _empty_candidate_plan()
                continue

            try:
                rank = int(row.get("rank") or 0)
            except Exception:
                rank = 0
            try:
                plan = compute_plan_for_code(
                    conn,
                    code=code,
                    rank=max(1, int(rank)),
                    signal_date=str(signal_date),
                    daytrade_cfg=daytrade_cfg,
                )
            except Exception:
                logging.exception("failed to compute selection candidate plan for %s", code)
                plan = None

            if plan:
                plan_by_code[code] = {
                    "status": "ready",
                    "signal_date": str(plan.signal_date),
                    "entry_price": round(float(plan.entry), 4),
                    "target_price": round(float(plan.target), 4),
                    "stop_price": round(float(plan.stop), 4),
                }
            else:
                plan_by_code[code] = _empty_candidate_plan("wait")

    cols = ["code", "name", "market", "amount", "close", "disparity", "rank", "sector_name", "industry_name"]
    for c in cols:
        if c not in final.columns:
            final[c] = None
    candidates = final[cols].replace([np.inf, -np.inf], np.nan).fillna("").to_dict(orient="records")
    for row in candidates:
        code = str(row.get("code") or "").strip().upper()
        row["plan"] = plan_by_code.get(code) or _empty_candidate_plan()

    def _items(df_stage: pd.DataFrame) -> List[Dict[str, Any]]:
        if df_stage.empty:
            return []
        out_cols = ["code", "name", "amount", "disparity"]
        for c in out_cols:
            if c not in df_stage.columns:
                df_stage[c] = None
        return df_stage[out_cols].head(15).replace([np.inf, -np.inf], np.nan).fillna("").to_dict(orient="records")

    stages = [
        {"key": "universe", "label": "Universe", "count": universe_total, "value": None},
        {"key": "min_amount", "label": "Min Amount", "count": int(len(stage_min_amount)), "value": min_amount},
        {"key": "liquidity", "label": "Liquidity", "count": int(len(stage_liquidity)), "value": liquidity_rank},
        {"key": "disparity", "label": "Disparity", "count": int(len(stage_disparity)), "value": {"nasdaq": buy_nasdaq, "sp500": buy_sp500}},
        {"key": "final", "label": "Final", "count": int(len(candidates)), "value": max_positions},
    ]

    stage_items = {
        "min_amount": _items(stage_min_amount),
        "liquidity": _items(stage_liquidity),
        "disparity": _items(stage_disparity),
        "final": _items(final),
    }

    changes = _build_selection_changes(conn, settings, params, toggles, latest_date)
    return {
        "date": latest_date,
        "candidates": candidates,
        "stages": stages,
        "stage_items": stage_items,
        "filter_toggles": toggles,
        "changes": changes,
    }


@app.get("/selection")
def selection():
    now = time.time()
    cached = _selection_cache.get("data")
    if cached and now - _selection_cache.get("ts", 0.0) < SELECTION_CACHE_TTL:
        return jsonify(cached)
    with _selection_lock:
        now = time.time()
        cached = _selection_cache.get("data")
        if cached and now - _selection_cache.get("ts", 0.0) < SELECTION_CACHE_TTL:
            return jsonify(cached)
        conn = get_conn()
        settings = load_settings()
        try:
            data = _build_selection_summary(conn, settings)
        except Exception:
            logging.exception("selection build failed")
            if cached:
                return jsonify(cached)
            raise
        finally:
            conn.close()
        _selection_cache["data"] = data
        _selection_cache["ts"] = time.time()
        return jsonify(data)


@app.get("/selection_filters")
def selection_filters():
    return jsonify(_load_filter_toggles())


@app.post("/selection_filters/toggle")
def selection_filters_toggle():
    payload = request.get_json(silent=True) or {}
    key = payload.get("key")
    enabled = bool(payload.get("enabled"))
    password = payload.get("password")
    if key not in FILTER_TOGGLE_KEYS:
        return jsonify({"error": "invalid key"}), 400
    if not _check_password(password):
        return jsonify({"error": "invalid password"}), 403
    toggles = _load_filter_toggles()
    toggles[key] = enabled
    return jsonify(_save_filter_toggles(toggles))


def _autotrade_optimize_default() -> bool:
    try:
        return bool(AUTOTRADE_CFG.get("optimize", True))
    except Exception:
        return True


def _autotrade_lookback_default() -> Optional[int]:
    try:
        v = AUTOTRADE_CFG.get("optimize_lookback_bars")
        return int(v) if v is not None else None
    except Exception:
        return None


@app.get("/autotrade/recommend")
def autotrade_recommend():
    """Return next-day entry/stop/target from stock_daytrade_engine (daily bars)."""
    code = str(request.args.get("code") or "").strip().upper()
    if not code:
        return jsonify({"ok": False, "error": "code is required"}), 400

    optimize_raw = request.args.get("optimize")
    if optimize_raw is None:
        optimize = _autotrade_optimize_default()
    else:
        optimize = str(optimize_raw).strip().lower() not in {"0", "false", "no"}

    lookback_raw = request.args.get("lookback")
    lookback = _autotrade_lookback_default()
    if lookback_raw is not None:
        try:
            lookback = int(lookback_raw)
        except Exception:
            lookback = lookback

    rec = recommend_daytrade_plan(
        db_path=str(DB_PATH),
        code=code,
        optimize=bool(optimize),
        optimize_lookback_bars=lookback,
    )

    if rec.get("ok"):
        snap = rec.get("snapshot") or {}
        plan = rec.get("plan") or {}
        asof_date = str(snap.get("date") or "")
        try:
            conn = get_conn()
            now = datetime.utcnow().isoformat()
            conn.execute(
                """
                INSERT INTO autotrade_plans(
                    asof_date, code, entry_price, target_price, stop_price, confidence, status, plan_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(asof_date, code) DO UPDATE SET
                    entry_price=excluded.entry_price,
                    target_price=excluded.target_price,
                    stop_price=excluded.stop_price,
                    confidence=excluded.confidence,
                    status=excluded.status,
                    plan_json=excluded.plan_json,
                    updated_at=excluded.updated_at;
                """,
                (
                    asof_date,
                    normalize_code(code),
                    _safe_float(plan.get("entry_price")),
                    _safe_float(plan.get("target_price")),
                    _safe_float(plan.get("stop_price")),
                    _safe_float(rec.get("confidence")),
                    str(rec.get("status") or ""),
                    json.dumps(rec, ensure_ascii=False, default=str),
                    now,
                    now,
                ),
            )
            conn.commit()
        except Exception:
            # Recommendation should still return; DB write is best-effort.
            logging.exception("failed to upsert autotrade plan for %s", code)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    return jsonify(rec)


@app.get("/autotrade/watchlist")
def autotrade_watchlist():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT code, name, market, excd, list_type, enabled, created_at, updated_at
            FROM autotrade_watchlist
            ORDER BY updated_at DESC
            """,
            conn,
        )
    except Exception:
        return jsonify([])
    finally:
        conn.close()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient="records"))


@app.post("/autotrade/watchlist/set")
def autotrade_watchlist_set():
    payload = request.get_json(silent=True) or {}
    password = payload.get("password")
    if not _check_autotrade_password(password):
        return jsonify({"error": "invalid password"}), 403

    code = normalize_code(payload.get("code"))
    list_type = str(payload.get("list_type") or LIST_SELECTED).strip().upper()
    enabled = bool(payload.get("enabled", True))
    if not code:
        return jsonify({"error": "code required"}), 400
    if list_type not in {LIST_SELECTED, LIST_EXIT}:
        return jsonify({"error": "invalid list_type"}), 400

    conn = get_conn()
    try:
        row = conn.execute("SELECT name, market, excd FROM universe_members WHERE code=?", (code,)).fetchone()
        name = str(payload.get("name") or (row[0] if row else "") or "").strip()
        market = str(payload.get("market") or (row[1] if row else "") or "").strip()
        excd = str(payload.get("excd") or (row[2] if row else "") or "").strip()
        now = datetime.utcnow().isoformat()
        conn.execute(
            """
            INSERT INTO autotrade_watchlist(code, name, market, excd, list_type, enabled, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                market=excluded.market,
                excd=excluded.excd,
                list_type=excluded.list_type,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at;
            """,
            (code, name, market, excd, list_type, int(enabled), now, now),
        )
        conn.commit()
        out = conn.execute(
            "SELECT code, name, market, excd, list_type, enabled, created_at, updated_at FROM autotrade_watchlist WHERE code=?",
            (code,),
        ).fetchone()
        return jsonify(dict(out) if out else {"code": code, "list_type": list_type, "enabled": enabled})
    finally:
        conn.close()


@app.post("/autotrade/watchlist/remove")
def autotrade_watchlist_remove():
    payload = request.get_json(silent=True) or {}
    password = payload.get("password")
    if not _check_autotrade_password(password):
        return jsonify({"error": "invalid password"}), 403
    code = normalize_code(payload.get("code"))
    if not code:
        return jsonify({"error": "code required"}), 400
    conn = get_conn()
    try:
        conn.execute("DELETE FROM autotrade_watchlist WHERE code=?", (code,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "code": code})


@app.get("/autotrade/queue")
def autotrade_queue():
    code = str(request.args.get("code") or "").strip().upper()
    conn = get_conn()
    try:
        if code:
            df = pd.read_sql_query(
                """
                SELECT id, asof_date, code, side, trigger_price, trigger_rule, status, attempt_count, last_attempt_at, sent_at, last_error, updated_at
                FROM autotrade_queue
                WHERE code=?
                ORDER BY asof_date DESC, id DESC
                LIMIT 200
                """,
                conn,
                params=(normalize_code(code),),
            )
        else:
            df = pd.read_sql_query(
                """
                SELECT id, asof_date, code, side, trigger_price, trigger_rule, status, attempt_count, last_attempt_at, sent_at, last_error, updated_at
                FROM autotrade_queue
                ORDER BY asof_date DESC, id DESC
                LIMIT 200
                """,
                conn,
            )
    except Exception:
        return jsonify([])
    finally:
        conn.close()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)
    return jsonify(df.to_dict(orient="records"))


def _list_known_sectors(conn: sqlite3.Connection) -> List[str]:
    """Return distinct sector names that already exist in DB (excluding 미분류/invalid tokens)."""
    rows = conn.execute(
        """
        SELECT DISTINCT trim(sector_name) AS sector_name
        FROM sector_map
        WHERE sector_name IS NOT NULL
          AND trim(sector_name) != ''
          AND lower(trim(sector_name)) NOT IN ('nan','none','null','na','n/a','unknown')
          AND trim(sector_name) != '미분류'
        ORDER BY sector_name
        """
    ).fetchall()
    out: List[str] = []
    for r in rows:
        try:
            name = str(r[0]).strip()
        except Exception:
            continue
        if name and name not in out:
            out.append(name)
    return out


@app.post("/sector_override")
def sector_override():
    """Manually classify a symbol's sector using an existing sector name."""
    payload = request.get_json(silent=True) or {}
    code = str(payload.get("code") or "").strip().upper()
    sector_name = str(payload.get("sector_name") or "").strip()
    password = payload.get("password")
    if not code:
        return jsonify({"error": "code required"}), 400
    if not sector_name:
        return jsonify({"error": "sector_name required"}), 400
    if not _check_password(password):
        return jsonify({"error": "invalid password"}), 403

    conn = get_conn()
    try:
        exists = conn.execute("SELECT 1 FROM universe_members WHERE code=? LIMIT 1", (code,)).fetchone()
        if not exists:
            return jsonify({"error": "unknown code"}), 404

        allowed = set(_list_known_sectors(conn))
        if sector_name == "UNKNOWN":
            sector_name = "미분류"
        if sector_name != "미분류" and sector_name not in allowed:
            return jsonify({"error": "sector_name must be one of existing sectors"}), 400

        row = conn.execute(
            "SELECT sector_code, industry_code FROM sector_map WHERE code=?",
            (code,),
        ).fetchone()
        sector_code = row[0] if row else None
        industry_code = row[1] if row else None
        industry_name = sector_name if sector_name != "미분류" else None
        now = datetime.utcnow().isoformat()

        conn.execute(
            """
            INSERT INTO sector_map(code, sector_code, sector_name, industry_code, industry_name, updated_at, source)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(code) DO UPDATE SET
                sector_name=excluded.sector_name,
                industry_name=excluded.industry_name,
                updated_at=excluded.updated_at,
                source=excluded.source;
            """,
            (code, sector_code, sector_name, industry_code, industry_name, now, "MANUAL_UI"),
        )
        conn.commit()
    finally:
        conn.close()

    # Ensure selection reflects updated sector constraints (max_per_sector).
    _selection_cache.update({"ts": 0.0, "data": None})
    return jsonify({"status": "success", "code": code, "sector_name": sector_name, "industry_name": industry_name})


@app.post("/client_error")
def client_error():
    payload = request.get_json(silent=True) or {}
    CLIENT_ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
    try:
        line = json.dumps({"ts": datetime.utcnow().isoformat(), **payload}, ensure_ascii=False)
        CLIENT_ERROR_LOG.write_text(
            (CLIENT_ERROR_LOG.read_text(encoding="utf-8") if CLIENT_ERROR_LOG.exists() else "") + line + "\n",
            encoding="utf-8",
        )
    except Exception:
        pass
    return jsonify({"status": "ok"})


@app.get("/status")
def status():
    now = time.time()
    with _status_cache_lock:
        cached_data = _status_cache.get("data")
        cached_ts = float(_status_cache.get("ts") or 0.0)
        cached_heavy_ts = float(_status_cache.get("heavy_ts") or 0.0)
        if cached_data and (now - cached_ts) < STATUS_CACHE_TTL:
            return jsonify(cached_data)

    conn = get_conn(timeout=0.2, busy_timeout_ms=200)
    try:
        prev_daily = ((cached_data or {}).get("daily_price") or {}) if isinstance(cached_data, dict) else {}
        collectors_running = _lock_file_active(WATCHDOG_DAILY_LOCK_PATH)
        rows_value = prev_daily.get("rows")
        if not collectors_running:
            rows_value = _count(conn, "daily_price")
        out = {
            "universe": {"total": _count(conn, "universe_members")},
            "daily_price": {
                "rows": rows_value,
                "codes": prev_daily.get("codes"),
                "missing_codes": prev_daily.get("missing_codes"),
                "date": prev_daily.get("date") or {"min": None, "max": None},
            },
            "jobs": {"recent": _count(conn, "job_runs")},
            "watchdog": _watchdog_snapshot(),
            "watchdog_external": _external_watchdog_state(),
            "watchdog_runtime": {"daily_lock_active": collectors_running},
        }

        need_heavy = (not prev_daily) or ((now - cached_heavy_ts) >= STATUS_HEAVY_INTERVAL_SEC)
        if need_heavy and not collectors_running:
            out["daily_price"]["rows"] = _count(conn, "daily_price")
            out["daily_price"]["codes"] = _distinct_code_count(conn, "daily_price")
            out["daily_price"]["missing_codes"] = _missing_codes(conn, "daily_price")
            out["daily_price"]["date"] = _minmax(conn, "daily_price")
            cached_heavy_ts = now

        with _status_cache_lock:
            _status_cache["data"] = out
            _status_cache["ts"] = now
            _status_cache["heavy_ts"] = cached_heavy_ts
        return jsonify(out)
    finally:
        conn.close()


@app.get("/jobs")
def jobs():
    _require_admin_or_404()
    conn = get_conn()
    limit = int(request.args.get("limit", 20))
    df = pd.read_sql_query("SELECT * FROM job_runs ORDER BY started_at DESC LIMIT ?", conn, params=(limit,))
    return jsonify(df.to_dict(orient="records"))


@app.get("/strategy")
def strategy():
    settings = load_settings()
    params = load_strategy(settings)
    return jsonify(
        {
            "entry_mode": params.entry_mode,
            "liquidity_rank": params.liquidity_rank,
            "min_amount": params.min_amount,
            "rank_mode": params.rank_mode,
            "disparity_buy_nasdaq100": params.buy_kospi,
            "disparity_buy_sp500": params.buy_kosdaq,
            "disparity_sell": params.sell_disparity,
            "take_profit_ret": params.take_profit_ret,
            "stop_loss": params.stop_loss,
            "max_holding_days": params.max_holding_days,
            "max_positions": params.max_positions,
            "max_per_sector": params.max_per_sector,
            "trend_ma25_rising": params.trend_ma25_rising,
            "selection_horizon_days": params.selection_horizon_days,
        }
    )


@app.post("/export")
def export_csv():
    _require_admin_or_404()
    settings = load_settings()
    if not (settings.get("export_csv") or {}).get("enabled", False):
        abort(404)
    maybe_export_db(settings, str(DB_PATH))
    return jsonify({"status": "success", "message": "CSV export completed"})


if __name__ == "__main__":
    host = os.getenv("BNF_VIEWER_HOST", "0.0.0.0")
    port = int(os.getenv("BNF_VIEWER_PORT", "5002"))
    app.run(host=host, port=port)
