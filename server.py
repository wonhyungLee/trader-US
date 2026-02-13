from __future__ import annotations

import os
import sqlite3
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, List

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory, g, abort
from flask_cors import CORS

from src.analyzer.backtest_runner import load_strategy
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings, list_kis_key_inventory, set_kis_key_enabled
from src.utils.db_exporter import maybe_export_db
from src.utils.project_root import ensure_repo_root

ensure_repo_root(Path(__file__).resolve().parent)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path("data/market_data.db")
FRONTEND_DIST = Path("frontend/dist")
UNIVERSE_CACHE_TTL_SEC = int(os.getenv("UNIVERSE_CACHE_TTL_SEC", "300"))
SECTORS_CACHE_TTL_SEC = int(os.getenv("SECTORS_CACHE_TTL_SEC", "300"))
SELECTION_CACHE_TTL_SEC = int(os.getenv("SELECTION_CACHE_TTL_SEC", "30"))
ACCOUNT_SNAPSHOT_PATH = Path("data/account_snapshot.json")
KIS_TOGGLE_PASSWORD = os.getenv("KIS_TOGGLE_PASSWORD", "lee37535**")
_universe_cache: Dict[str, Any] = {"ts": 0.0, "rows": None}
_sectors_cache: Dict[str, Any] = {"ts": 0.0, "rows": None}
_selection_cache: Dict[str, Any] = {"ts": 0.0, "data": None}
_balance_cache: Dict[str, Any] = {"ts": 0.0, "data": None}


_store = SQLiteStore(str(DB_PATH))
_store.conn.close()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    return conn


def get_db() -> sqlite3.Connection:
    db = getattr(g, "_db", None)
    if db is None:
        db = get_conn()
        setattr(g, "_db", db)
    return db


def _close_db(exc: Exception | None = None) -> None:
    db = getattr(g, "_db", None)
    if db is not None:
        try:
            db.close()
        finally:
            try:
                delattr(g, "_db")
            except Exception:
                pass


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
    codes: List[str] = []
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


def _check_password(password: Optional[str]) -> bool:
    if not KIS_TOGGLE_PASSWORD:
        return True
    return bool(password) and password == KIS_TOGGLE_PASSWORD


cors_origins = os.getenv("CORS_ORIGINS", "").strip()
if cors_origins:
    origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
    if origins:
        CORS(app, resources={r"/*": {"origins": origins}})

app.teardown_appcontext(_close_db)

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
    conn = get_db()
    sector = request.args.get("sector")
    now_ts = time.time()

    cached = _universe_cache.get("rows")
    if cached is not None and now_ts - float(_universe_cache.get("ts") or 0.0) < UNIVERSE_CACHE_TTL_SEC:
        rows = cached
    else:
        try:
            df = pd.read_sql_query(
                """
                SELECT u.code, u.name, u.market, u.group_name as 'group',
                       COALESCE(s.sector_name, 'UNKNOWN') AS sector_name,
                       s.industry_name
                FROM universe_members u
                LEFT JOIN sector_map s ON u.code = s.code
                ORDER BY u.code
                """,
                conn,
            )
        except Exception:
            df = pd.read_sql_query(
                "SELECT code, name, market, group_name as 'group' FROM universe_members ORDER BY code",
                conn,
            )
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object).where(pd.notnull(df), None)
        rows = df.to_dict(orient="records")
        _universe_cache.update({"ts": now_ts, "rows": rows})

    if sector and sector != "ALL":
        if str(sector).upper() == "UNKNOWN":
            rows = [r for r in rows if str(r.get("sector_name") or "UNKNOWN").upper() == "UNKNOWN"]
        else:
            rows = [r for r in rows if str(r.get("sector_name") or "UNKNOWN") == str(sector)]
    return jsonify(rows)



@app.get("/sectors")
def sectors():
    conn = get_db()
    now_ts = time.time()
    cached = _sectors_cache.get("rows")
    if cached is not None and now_ts - float(_sectors_cache.get("ts") or 0.0) < SECTORS_CACHE_TTL_SEC:
        rows = cached
    else:
        try:
            df = pd.read_sql_query(
                """
                SELECT u.market,
                       COALESCE(s.sector_name, 'UNKNOWN') AS sector_name,
                       COUNT(*) AS count
                FROM universe_members u
                LEFT JOIN sector_map s ON u.code = s.code
                GROUP BY u.market, COALESCE(s.sector_name, 'UNKNOWN')
                ORDER BY u.market, count DESC, sector_name
                """,
                conn,
            )
        except Exception:
            df = pd.DataFrame([], columns=["market", "sector_name", "count"])
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object).where(pd.notnull(df), None)
        rows = df.to_dict(orient="records")
        _sectors_cache.update({"ts": now_ts, "rows": rows})
    return jsonify(rows)



@app.get("/prices")
def prices():
    code = request.args.get("code")
    days = int(request.args.get("days", 360))
    if not code:
        return jsonify([])

    conn = get_db()
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


@app.get("/portfolio")
def portfolio():
    conn = get_db()
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
    conn = get_db()
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
                   o.ord_dvsn, o.ord_unpr, o.created_at, o.updated_at,
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
    conn = get_db()
    settings = load_settings()
    return jsonify(_build_account_summary(conn, settings))


@app.get("/kis_keys")
def kis_keys():
    inventory = list_kis_key_inventory()
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
    enriched = []
    for item in updated:
        row = dict(item)
        row["account"] = item.get("account_no_masked") or item.get("label")
        row.setdefault("env", "real")
        enriched.append(row)
    return jsonify(enriched)


def _build_selection_summary(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Buy candidates only (no auto-trading / no account)."""
    params = load_strategy(settings)

    min_amount = float(getattr(params, "min_amount", 0) or 0)
    liquidity_rank = int(getattr(params, "liquidity_rank", 0) or 0)
    # Reuse existing param names but expose as NASDAQ100/SP500 in API
    buy_nasdaq = float(getattr(params, "buy_kospi", 0) or 0)
    buy_sp500 = float(getattr(params, "buy_kosdaq", 0) or 0)
    max_positions = int(getattr(params, "max_positions", 20) or 20)
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    trend_filter = bool(getattr(params, "trend_ma25_rising", False))

    universe_total = int(conn.execute("SELECT COUNT(*) FROM universe_members").fetchone()[0] or 0)
    group_map = {row[0]: row[1] for row in conn.execute("SELECT code, group_name FROM universe_members").fetchall()}

    sql = """
        WITH recent AS (
            SELECT d.code, d.date, d.close, d.amount, d.ma25, d.disparity,
                   u.name, u.market, u.group_name,
                   ROW_NUMBER() OVER (PARTITION BY d.code ORDER BY d.date DESC) AS rn_desc
            FROM daily_price d
            JOIN universe_members u ON u.code = d.code
        ),
        calc AS (
            SELECT code, date, close, amount, ma25, disparity, name, market, group_name,
                   LAG(ma25,1) OVER (PARTITION BY code ORDER BY date) AS ma25_prev,
                   (close / LAG(close,3) OVER (PARTITION BY code ORDER BY date) - 1.0) AS ret3,
                   rn_desc
            FROM recent
            WHERE rn_desc <= 4
        )
        SELECT code, date, close, amount, ma25, disparity, ma25_prev, ret3, name, market, group_name
        FROM calc
        WHERE rn_desc = 1
    """
    latest = pd.read_sql_query(sql, conn)
    if latest.empty:
        return {"date": None, "candidates": [], "summary": {"total": universe_total, "final": 0}}

    total = len(latest)

    stage = latest
    # Attach sector/industry early so max_per_sector works as intended.
    try:
        sector_df = pd.read_sql_query(
            "SELECT code, COALESCE(sector_name,'UNKNOWN') AS sector_name, industry_name FROM sector_map",
            conn,
        )
        stage = stage.merge(sector_df, on="code", how="left")
    except Exception:
        stage["sector_name"] = None
        stage["industry_name"] = None
    if min_amount:
        stage = stage[stage["amount"] >= min_amount]

    stage = stage.sort_values("amount", ascending=False)
    if liquidity_rank:
        stage = stage.head(liquidity_rank)

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

    stage = stage[stage.apply(pass_signal, axis=1)]
    if trend_filter:
        stage = stage[stage["ma25_prev"].notna() & (stage["ma25"] > stage["ma25_prev"])]

    ranked = stage.copy()
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
                   COALESCE(s.sector_name, u.group_name, 'UNKNOWN') AS sec
            FROM position_state p
            LEFT JOIN sector_map s ON p.code = s.code
            LEFT JOIN universe_members u ON p.code = u.code
            """
        ).fetchall()
        for code, sec in held:
            sec = sec or "UNKNOWN"
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}
    for _, row in ranked.iterrows():
        sec = row.get("sector_name") or "UNKNOWN"
        if max_per_sector and sector_counts.get(sec, 0) >= max_per_sector:
            continue
        final_rows.append(row)
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(final_rows) >= max_positions:
            break

    final = pd.DataFrame(final_rows) if final_rows else ranked.head(0).copy()
    if not final.empty:
        final["rank"] = range(1, len(final) + 1)

    try:
        sector_df = pd.read_sql_query("SELECT code, sector_name, industry_name FROM sector_map", conn)
        final = final.merge(sector_df, on="code", how="left")
    except Exception:
        pass

    latest_date = latest["date"].max()
    cols = ["code", "name", "market", "group_name", "amount", "close", "disparity", "rank", "sector_name", "industry_name"]
    for c in cols:
        if c not in final.columns:
            final[c] = None
    candidates = final[cols].replace([np.inf, -np.inf], np.nan).fillna("").to_dict(orient="records")

    return {
        "date": latest_date,
        "candidates": candidates,
        "summary": {
            "total": int(total),
            "final": int(len(candidates)),
            "entry_mode": entry_mode,
            "rank_mode": rank_mode,
            "liquidity_rank": liquidity_rank,
            "min_amount": min_amount,
            "buy_thresholds": {"nasdaq100": buy_nasdaq, "sp500": buy_sp500},
            "trend_filter": trend_filter,
            "max_positions": max_positions,
            "max_per_sector": max_per_sector,
        },
    }


@app.get("/selection")
def selection():
    now_ts = time.time()
    cached = _selection_cache.get("data")
    if cached is not None and now_ts - float(_selection_cache.get("ts") or 0.0) < SELECTION_CACHE_TTL_SEC:
        return jsonify(cached)

    conn = get_db()
    settings = load_settings()
    data = _build_selection_summary(conn, settings)
    try:
        data = json.loads(json.dumps(data, allow_nan=False))
    except Exception:
        pass
    _selection_cache.update({"ts": now_ts, "data": data})
    return jsonify(data)





@app.get("/status")
def status():
    conn = get_db()
    out = {
        "universe": {"total": _count(conn, "universe_members")},
        "daily_price": {
            "rows": _count(conn, "daily_price"),
            "codes": _distinct_code_count(conn, "daily_price"),
            "missing_codes": _missing_codes(conn, "daily_price"),
            "date": _minmax(conn, "daily_price"),
        },
        "jobs": {"recent": _count(conn, "job_runs")},
    }
    return jsonify(out)


@app.get("/jobs")
def jobs():
    _require_admin_or_404()
    conn = get_db()
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
