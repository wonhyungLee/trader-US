from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Holiday,
    nearest_workday,
    USMartinLutherKingJr,
    USPresidentsDay,
    GoodFriday,
    USMemorialDay,
    USLaborDay,
    USThanksgivingDay,
)
from pandas.tseries.offsets import CustomBusinessDay

from src.analyzer.backtest_runner import load_strategy
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings, load_yaml

from .indicators import rolling_sma, rsi_sma, atr_sma


ACCOUNT_SNAPSHOT_PATH = Path("data/account_snapshot.json")


class _NYSEHolidayCalendar(AbstractHolidayCalendar):
    """A lightweight NYSE holiday calendar for exec_date calculation.

    This is *not* a perfect market calendar (it does not cover ad-hoc closures),
    but it correctly skips standard NYSE holidays such as Presidents Day and Good Friday.
    """

    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, observance=nearest_workday, start_date="2021-06-19"),
        Holiday("IndependenceDay", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


_NYSE_BDAY = CustomBusinessDay(calendar=_NYSEHolidayCalendar())


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
    except Exception:
        return None


def _load_strategy_yaml() -> Dict[str, Any]:
    p = Path("config/strategy.yaml")
    if p.exists():
        return load_yaml(str(p))
    return {}


def load_daytrade_cfg(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load daytrade config from config/strategy.yaml (preferred) or settings."""
    settings = settings or load_settings()
    strat = _load_strategy_yaml()
    daytrade = strat.get("daytrade")
    if isinstance(daytrade, dict):
        return daytrade
    # fallback: allow settings.yaml to carry the same structure
    daytrade = (settings.get("daytrade") or {})
    return daytrade if isinstance(daytrade, dict) else {}


def latest_price_date(conn: sqlite3.Connection) -> Optional[str]:
    try:
        row = conn.execute("SELECT MAX(date) FROM daily_price").fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def next_business_day(date_ymd: str) -> str:
    """Return next NYSE business day.

    Skips weekends + standard NYSE holidays (e.g., Presidents Day, Good Friday).
    Exceptional closures (e.g., national mourning days) are not included.
    """
    d = pd.Timestamp(date_ymd)
    nd = (d + _NYSE_BDAY).date()
    return str(nd)


def _load_account_snapshot_total() -> Optional[float]:
    if not ACCOUNT_SNAPSHOT_PATH.exists():
        return None
    try:
        payload = json.loads(ACCOUNT_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    # "initial_total" is stored by server.py when account endpoint first succeeds
    return _safe_float(payload.get("initial_total"))


def resolve_total_assets(
    settings: Dict[str, Any],
    *,
    override_total_assets: Optional[float] = None,
) -> float:
    """Resolve total assets used for position sizing.

    Priority:
      1) explicit override
      2) env DAYTRADE_TOTAL_ASSETS
      3) data/account_snapshot.json (if exists)
      4) strategy.position.initial_cash (fallback)
    """
    if override_total_assets is not None:
        return float(override_total_assets)

    env_v = os.getenv("DAYTRADE_TOTAL_ASSETS")
    if env_v:
        try:
            return float(env_v)
        except Exception:
            pass

    snap = _load_account_snapshot_total()
    if snap is not None:
        return float(snap)

    # fallback (viewer에서도 존재)
    try:
        strat = _load_strategy_yaml()
        pos = strat.get("position") or {}
        v = _safe_float(pos.get("initial_cash"))
        if v is not None:
            return float(v)
    except Exception:
        pass
    return 0.0


def build_traderus_selection(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Tuple[Optional[str], pd.DataFrame]:
    """Reproduce /selection logic (so CLI can run without Flask server).

    Returns (latest_date, final_candidates_df)
    """
    params = load_strategy(settings)

    min_amount = float(getattr(params, "min_amount", 0) or 0)
    liquidity_rank = int(getattr(params, "liquidity_rank", 0) or 0)
    buy_nasdaq = float(getattr(params, "buy_kospi", 0) or 0)
    buy_sp500 = float(getattr(params, "buy_kosdaq", 0) or 0)
    max_positions = int(getattr(params, "max_positions", 20) or 20)
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    trend_filter = bool(getattr(params, "trend_ma25_rising", False))

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
        return None, latest

    # Attach sector/industry early (so max_per_sector works)
    stage = latest
    try:
        sector_df = pd.read_sql_query(
            "SELECT code, COALESCE(sector_name,'UNKNOWN') AS sector_name, industry_name FROM sector_map",
            conn,
        )
        stage = stage.merge(sector_df, on="code", how="left")
    except Exception:
        stage = stage.copy()
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
    # If position_state exists, include it in sector exposure (defensive)
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
        for _code, sec in held:
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

    latest_date = str(latest["date"].max())
    return latest_date, final


def _fetch_ohlc(conn: sqlite3.Connection, code: str, limit: int) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT date, open, high, low, close
        FROM daily_price
        WHERE code=?
        ORDER BY date DESC
        LIMIT ?
        """,
        conn,
        params=(code, int(limit)),
    )
    if df.empty:
        return df
    return df.sort_values("date").reset_index(drop=True)


@dataclass
class DaytradePlan:
    code: str
    rank: int
    signal_date: str
    close: float
    atr: float
    atr_pct: float
    sma_fast: float
    rsi: float
    entry: float
    stop: float
    target: float


def compute_plan_for_code(
    conn: sqlite3.Connection,
    *,
    code: str,
    rank: int,
    signal_date: str,
    daytrade_cfg: Dict[str, Any],
    min_bars: int = 260,
    enforce_signal_trigger: bool = True,
    fallback_to_latest_before_signal_date: bool = False,
) -> Optional[DaytradePlan]:
    sig = daytrade_cfg.get("signal") or {}
    br = daytrade_cfg.get("bracket") or {}

    rsi_period = int(sig.get("rsi_period", 2))
    rsi_thresh = float(sig.get("rsi_thresh", 10))
    sma_fast_n = int(sig.get("sma_fast", 5))
    use_trend_filter = bool(sig.get("use_trend_filter", False))
    sma_trend_n = int(sig.get("sma_trend", 200))
    atr_n = int(sig.get("atr_period", 14))

    entry_k = float(br.get("entry_k", 1.0))
    stop_mult = float(br.get("stop_mult", 1.75))
    target_mult = float(br.get("target_mult", 0.75))

    need = max(min_bars, sma_fast_n + 5, atr_n + 5, rsi_period + 5, (sma_trend_n + 5) if use_trend_filter else 0)
    df = _fetch_ohlc(conn, code, limit=need)
    if df.empty or len(df) < max(atr_n + 2, sma_fast_n + 2, rsi_period + 2, (sma_trend_n + 2) if use_trend_filter else 0):
        return None

    # Align to signal_date (defensive; usually the latest row)
    dates = df["date"].astype(str)
    if signal_date in set(dates):
        i = int(df.index[dates == signal_date][0])
    elif fallback_to_latest_before_signal_date:
        candidates = df.index[dates <= str(signal_date)].tolist()
        if not candidates:
            return None
        i = int(candidates[-1])
        signal_date = str(df.iloc[i]["date"])
    else:
        # if the code has missing latest date, skip
        return None
    if i <= 0:
        return None

    h = df["high"].astype(float).to_numpy()
    l = df["low"].astype(float).to_numpy()
    c = df["close"].astype(float).to_numpy()

    sma_fast = rolling_sma(c, sma_fast_n)
    rsi = rsi_sma(c, rsi_period)
    atr = atr_sma(h, l, c, atr_n)
    sma_trend = rolling_sma(c, sma_trend_n) if use_trend_filter else None

    if any(np.isnan(x) for x in (sma_fast[i], rsi[i], atr[i])):
        return None
    if use_trend_filter and (sma_trend is None or np.isnan(float(sma_trend[i]))):
        return None

    close = float(c[i])
    atr_v = float(atr[i])
    atr_pct = (atr_v / close * 100.0) if close else float("nan")

    trigger = (close < float(sma_fast[i])) and (float(rsi[i]) <= float(rsi_thresh))
    if use_trend_filter:
        trigger = trigger and (close > float(sma_trend[i]))
    if enforce_signal_trigger and not trigger:
        return None

    entry = close - entry_k * atr_v
    stop = entry - stop_mult * atr_v
    target = entry + target_mult * atr_v

    return DaytradePlan(
        code=str(code),
        rank=int(rank),
        signal_date=str(signal_date),
        close=close,
        atr=atr_v,
        atr_pct=float(atr_pct),
        sma_fast=float(sma_fast[i]),
        rsi=float(rsi[i]),
        entry=float(entry),
        stop=float(stop),
        target=float(target),
    )


def build_orders_from_plans(
    plans: List[DaytradePlan],
    *,
    daytrade_cfg: Dict[str, Any],
    total_assets: float,
    exec_date: str,
    signal_date: str,
) -> List[Dict[str, Any]]:
    ex = daytrade_cfg.get("execution") or {}
    alloc_pct = float(ex.get("alloc_pct", 0.2))
    ord_dvsn = str(ex.get("ord_dvsn", "00"))
    fee_bps = float(ex.get("fee_bps", 20))
    mode = str(daytrade_cfg.get("mode", "balanced"))
    both_hit_rule = str((daytrade_cfg.get("bracket") or {}).get("both_hit_rule", "stop_first"))

    alloc_cash = float(total_assets) * alloc_pct if total_assets else 0.0

    orders: List[Dict[str, Any]] = []
    for p in plans:
        entry_price = float(p.entry)
        if entry_price <= 0:
            continue
        qty = int(alloc_cash // entry_price) if alloc_cash > 0 else 0
        if qty <= 0:
            continue

        meta = {
            "strategy": "TraderUS+Daytrade",
            "mode": mode,
            "signal_date": signal_date,
            "exec_date": exec_date,
            "indicators": {
                "close": round(float(p.close), 6),
                "sma_fast": round(float(p.sma_fast), 6),
                "rsi": round(float(p.rsi), 6),
                "atr": round(float(p.atr), 6),
                "atr_pct": round(float(p.atr_pct), 4),
            },
            "bracket": {
                "entry": round(float(p.entry), 6),
                "stop": round(float(p.stop), 6),
                "target": round(float(p.target), 6),
                "both_hit_rule": both_hit_rule,
                "exit_rule": "EOD",
            },
            "costs": {
                "fee_bps_per_side": fee_bps,
            },
            "sizing": {
                "total_assets": total_assets,
                "alloc_pct": alloc_pct,
                "alloc_cash": alloc_cash,
                "qty": qty,
            },
        }

        orders.append(
            {
                "signal_date": signal_date,
                "code": p.code,
                "side": "BUY",
                "qty": qty,
                "rank": p.rank,
                "ord_dvsn": ord_dvsn,
                "ord_unpr": round(float(p.entry), 4),
                "stop_unpr": round(float(p.stop), 4),
                "target_unpr": round(float(p.target), 4),
                "strategy": "DAYTRADE_BALANCED" if mode == "balanced" else f"DAYTRADE_{mode.upper()}",
                "meta_json": json.dumps(meta, ensure_ascii=False),
            }
        )
    return orders


def generate_daytrade_orders(
    store: SQLiteStore,
    *,
    settings: Optional[Dict[str, Any]] = None,
    signal_date: Optional[str] = None,
    exec_date: Optional[str] = None,
    total_assets_override: Optional[float] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    daytrade_cfg = load_daytrade_cfg(settings)
    if not bool(daytrade_cfg.get("enabled", False)):
        return {"ok": False, "reason": "daytrade_disabled"}

    sig_date = signal_date or latest_price_date(store.conn)
    if not sig_date:
        return {"ok": False, "reason": "no_price_data"}
    ex_date = exec_date or next_business_day(sig_date)

    # 1) TraderUS selection
    latest_sel_date, sel_df = build_traderus_selection(store.conn, settings)
    if sel_df.empty:
        return {"ok": False, "reason": "no_selection_candidates", "signal_date": sig_date}
    if latest_sel_date and latest_sel_date != sig_date:
        logging.warning("Selection latest date (%s) differs from daily_price max date (%s)", latest_sel_date, sig_date)

    # 2) Daytrade triggers + plans
    ex = daytrade_cfg.get("execution") or {}
    max_orders = int(ex.get("max_orders_per_day", 5))
    min_atr_pct = float(ex.get("min_atr_pct", 0.0) or 0.0)

    plans: List[DaytradePlan] = []
    for _, row in sel_df.iterrows():
        code = str(row.get("code"))
        rank = int(row.get("rank") or 0)
        p = compute_plan_for_code(
            store.conn,
            code=code,
            rank=rank,
            signal_date=sig_date,
            daytrade_cfg=daytrade_cfg,
        )
        if not p:
            continue
        if (min_atr_pct > 0) and (float(p.atr_pct) < min_atr_pct):
            continue
        plans.append(p)

    plans.sort(key=lambda x: x.rank)
    plans = plans[: max_orders]

    total_assets = resolve_total_assets(settings, override_total_assets=total_assets_override)
    orders = build_orders_from_plans(
        plans,
        daytrade_cfg=daytrade_cfg,
        total_assets=total_assets,
        exec_date=ex_date,
        signal_date=sig_date,
    )

    # Export plan for review
    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    plan_rows = [p.__dict__ for p in plans]
    plan_path = out_dir / f"daytrade_plans_{sig_date}.csv"
    try:
        pd.DataFrame(plan_rows).to_csv(plan_path, index=False)
    except Exception:
        pass

    if not dry_run:
        store.add_pending_orders(orders, ex_date)

    return {
        "ok": True,
        "signal_date": sig_date,
        "exec_date": ex_date,
        "selection_count": int(len(sel_df)),
        "triggered_count": int(len(plans)),
        "orders_count": int(len(orders)),
        "total_assets": total_assets,
        "plan_csv": str(plan_path),
        "orders": orders,
    }
