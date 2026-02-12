from __future__ import annotations

import os
import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from src.analyzer.backtest_runner import load_strategy
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db
from src.utils.project_root import ensure_repo_root

ensure_repo_root(Path(__file__).resolve().parent)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path("data/market_data.db")
FRONTEND_DIST = Path("frontend/dist")

_store = SQLiteStore(str(DB_PATH))
_store.conn.close()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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


app = Flask(__name__, static_folder=str(FRONTEND_DIST), static_url_path="")
CORS(app, resources={r"/*": {"origins": "*"}})

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
        if sector.upper() == "UNKNOWN":
            where = "s.sector_name IS NULL"
            params: Tuple[Any, ...] = ()
        else:
            where = "s.sector_name = ?"
            params = (sector,)
    else:
        where = "1=1"
        params = ()

    try:
        df = pd.read_sql_query(
            f"""
            SELECT u.code, u.name, u.market, u.group_name as 'group',
                   COALESCE(s.sector_name, 'UNKNOWN') AS sector_name,
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
    return jsonify(df.to_dict(orient="records"))


@app.get("/sectors")
def sectors():
    conn = get_conn()
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

    universe_df = pd.read_sql_query("SELECT code, name, market, group_name FROM universe_members", conn)
    codes = universe_df["code"].dropna().astype(str).tolist()
    if not codes:
        return {"date": None, "candidates": [], "summary": {"total": 0, "final": 0}}
    group_map = dict(zip(universe_df["code"], universe_df["group_name"]))

    placeholder = ",".join("?" * len(codes))
    sql = f"""
        SELECT code, date, close, amount, ma25, disparity
        FROM (
            SELECT code, date, close, amount, ma25, disparity,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_price
            WHERE code IN ({placeholder})
        )
        WHERE rn <= 4
    """
    df = pd.read_sql_query(sql, conn, params=codes)
    if df.empty:
        return {"date": None, "candidates": [], "summary": {"total": len(codes), "final": 0}}

    df = df.sort_values(["code", "date"])
    df["ma25_prev"] = df.groupby("code")["ma25"].shift(1)
    df["ret3"] = df.groupby("code")["close"].pct_change(3)
    latest = df.groupby("code").tail(1).copy()
    latest = latest.merge(universe_df, on="code", how="left")

    total = len(latest)

    stage = latest
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
        held = conn.execute("SELECT code FROM position_state").fetchall()
        for (code,) in held:
            sec = group_map.get(code) or "UNKNOWN"
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}
    for _, row in ranked.iterrows():
        sec = row.get("sector_name") or row.get("group_name") or "UNKNOWN"
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
    conn = get_conn()
    settings = load_settings()
    return jsonify(_build_selection_summary(conn, settings))


@app.get("/status")
def status():
    conn = get_conn()
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
    settings = load_settings()
    maybe_export_db(settings, str(DB_PATH))
    return jsonify({"status": "success", "message": "CSV export completed"})


if __name__ == "__main__":
    host = os.getenv("BNF_VIEWER_HOST", "0.0.0.0")
    port = int(os.getenv("BNF_VIEWER_PORT", "5002"))
    app.run(host=host, port=port)
