#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.analyzer.backtest_runner import load_strategy
from src.daytrade.indicators import atr_sma, rolling_sma, rsi_sma
from src.daytrade.planner import load_daytrade_cfg
from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


@dataclass
class YearMetrics:
    year: int
    start_date: str
    end_date: str
    n_days: int
    selection_count: int
    trigger_count: int
    trade_count: int
    win_rate: float
    total_return: float
    mdd: float
    pnl: float
    end_equity: float


def _load_prices_with_universe(
    conn,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    sql = """
    SELECT
        d.date,
        d.code,
        CAST(d.open AS REAL) AS open,
        CAST(d.high AS REAL) AS high,
        CAST(d.low AS REAL) AS low,
        CAST(d.close AS REAL) AS close,
        CAST(d.amount AS REAL) AS amount,
        CAST(d.ma25 AS REAL) AS ma25,
        CAST(d.disparity AS REAL) AS disparity,
        COALESCE(u.market, '') AS market,
        COALESCE(u.group_name, 'UNKNOWN') AS group_name,
        COALESCE(sm.industry_name, sm.sector_name, u.group_name, 'UNKNOWN') AS sector_name
    FROM daily_price d
    JOIN universe_members u
      ON d.code = u.code
    LEFT JOIN sector_map sm
      ON d.code = sm.code
    WHERE d.date >= ? AND d.date <= ?
    """
    df = pd.read_sql_query(sql, conn, params=(start_date, end_date))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["code"] = df["code"].astype(str).str.upper().str.strip()
    return df


def _compute_indicators(df: pd.DataFrame, *, sma_fast_n: int, atr_n: int, rsi_n: int, sma_trend_n: int, use_trend_filter: bool) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values(["code", "date"]).reset_index(drop=True)

    out["ret3"] = out.groupby("code")["close"].pct_change(3)
    out["ma25_prev"] = out.groupby("code")["ma25"].shift(1)

    out["sma_fast_sig"] = np.nan
    out["rsi_sig"] = np.nan
    out["atr_sig"] = np.nan
    out["sma_trend_sig"] = np.nan

    for code, g in out.groupby("code", sort=False):
        idx = g.index.to_numpy()
        c = g["close"].to_numpy(dtype=float)
        h = g["high"].to_numpy(dtype=float)
        l = g["low"].to_numpy(dtype=float)

        out.loc[idx, "sma_fast_sig"] = rolling_sma(c, sma_fast_n)
        out.loc[idx, "rsi_sig"] = rsi_sma(c, rsi_n)
        out.loc[idx, "atr_sig"] = atr_sma(h, l, c, atr_n)
        if use_trend_filter:
            out.loc[idx, "sma_trend_sig"] = rolling_sma(c, sma_trend_n)

    out = out.sort_values(["date", "code"]).reset_index(drop=True)
    return out


def _selection_for_day(
    day_df: pd.DataFrame,
    *,
    entry_mode: str,
    rank_mode: str,
    min_amount: float,
    liquidity_rank: int,
    buy_nasdaq: float,
    buy_sp500: float,
    trend_ma25_rising: bool,
    max_positions: int,
    max_per_sector: int,
) -> pd.DataFrame:
    if day_df.empty:
        return day_df.head(0).copy()

    stage = day_df.copy()
    stage = stage[stage["amount"] >= min_amount]
    if stage.empty:
        return stage

    stage = stage.sort_values("amount", ascending=False).head(liquidity_rank)
    group = (stage["group_name"].fillna(stage["market"]).astype(str).str.upper())
    th = np.where(group.str.contains("NASDAQ"), buy_nasdaq, buy_sp500)

    if entry_mode == "trend_follow":
        cond = (stage["disparity"] >= th) & (stage["ret3"].fillna(0) >= 0)
    else:
        cond = stage["disparity"] <= th

    if trend_ma25_rising:
        cond &= stage["ma25_prev"].notna() & (stage["ma25"] > stage["ma25_prev"])

    stage = stage[cond].copy()
    if stage.empty:
        return stage

    if rank_mode == "score":
        if entry_mode == "trend_follow":
            stage["score"] = (
                stage["disparity"].fillna(0).astype(float)
                + (0.8 * stage["ret3"].fillna(0).astype(float))
                + (0.05 * np.log1p(stage["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        else:
            stage["score"] = (
                -stage["disparity"].fillna(0).astype(float)
                + (0.8 * -stage["ret3"].fillna(0).astype(float))
                + (0.05 * np.log1p(stage["amount"].fillna(0).astype(float).clip(lower=0)))
            )
        stage = stage.sort_values("score", ascending=False)
    else:
        stage = stage.sort_values("amount", ascending=False)

    selected_rows = []
    sector_counts: Dict[str, int] = {}
    for _, row in stage.iterrows():
        sec = str(row.get("sector_name") or "UNKNOWN")
        if max_per_sector > 0 and sector_counts.get(sec, 0) >= max_per_sector:
            continue
        selected_rows.append(row)
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(selected_rows) >= max_positions:
            break

    if not selected_rows:
        return stage.head(0).copy()
    out = pd.DataFrame(selected_rows).copy()
    out["rank"] = range(1, len(out) + 1)
    return out


def _simulate_one_year(
    prices_all: pd.DataFrame,
    *,
    year_start: pd.Timestamp,
    year_end: pd.Timestamp,
    initial_cash: float,
    use_dynamic_sizing: bool,
    entry_mode: str,
    rank_mode: str,
    min_amount: float,
    liquidity_rank: int,
    buy_nasdaq: float,
    buy_sp500: float,
    trend_ma25_rising: bool,
    max_positions: int,
    max_per_sector: int,
    rsi_thresh: float,
    entry_k: float,
    stop_mult: float,
    target_mult: float,
    use_daytrade_trend: bool,
    min_atr_pct: float,
    max_orders_per_day: int,
    fee_bps: float,
    both_hit_rule: str,
    alloc_pct: float,
) -> YearMetrics:
    dates_all = sorted(prices_all["date"].drop_duplicates().tolist())
    dates = [d for d in dates_all if (d >= year_start and d <= year_end)]
    if len(dates) < 2:
        return YearMetrics(
            year=int(year_start.year),
            start_date=str(year_start.date()),
            end_date=str(year_end.date()),
            n_days=0,
            selection_count=0,
            trigger_count=0,
            trade_count=0,
            win_rate=0.0,
            total_return=0.0,
            mdd=0.0,
            pnl=0.0,
            end_equity=initial_cash,
        )

    day_map: Dict[pd.Timestamp, pd.DataFrame] = {
        d: g.reset_index(drop=True) for d, g in prices_all.groupby("date", sort=False)
    }

    equity = float(initial_cash)
    equity_rows: List[Tuple[str, float]] = [(str(dates[0].date()), equity)]
    trade_pnls: List[float] = []
    selection_count = 0
    trigger_count = 0

    fee = float(fee_bps) / 10000.0
    entry_fee_mult = 1.0 + fee
    exit_fee_mult = 1.0 - fee

    for i in range(len(dates) - 1):
        d = dates[i]
        nd = dates[i + 1]

        day_df = day_map.get(d)
        next_df = day_map.get(nd)
        if day_df is None or next_df is None or day_df.empty or next_df.empty:
            equity_rows.append((str(nd.date()), equity))
            continue

        selected = _selection_for_day(
            day_df,
            entry_mode=entry_mode,
            rank_mode=rank_mode,
            min_amount=min_amount,
            liquidity_rank=liquidity_rank,
            buy_nasdaq=buy_nasdaq,
            buy_sp500=buy_sp500,
            trend_ma25_rising=trend_ma25_rising,
            max_positions=max_positions,
            max_per_sector=max_per_sector,
        )
        if selected.empty:
            equity_rows.append((str(nd.date()), equity))
            continue

        selection_count += int(len(selected))
        next_map = next_df.set_index("code")

        triggered_today = []
        for _, row in selected.iterrows():
            close = float(row.get("close") or 0.0)
            sma_fast = row.get("sma_fast_sig")
            rsi = row.get("rsi_sig")
            atr = row.get("atr_sig")
            if close <= 0 or pd.isna(sma_fast) or pd.isna(rsi) or pd.isna(atr):
                continue

            if not (close < float(sma_fast) and float(rsi) <= float(rsi_thresh)):
                continue

            if use_daytrade_trend:
                sma_trend = row.get("sma_trend_sig")
                if pd.isna(sma_trend) or not (close > float(sma_trend)):
                    continue

            atr_v = float(atr)
            atr_pct = (atr_v / close * 100.0) if close else float("nan")
            if min_atr_pct > 0 and (math.isnan(atr_pct) or atr_pct < min_atr_pct):
                continue

            entry = close - (entry_k * atr_v)
            stop = entry - (stop_mult * atr_v)
            target = entry + (target_mult * atr_v)
            if entry <= 0:
                continue

            triggered_today.append(
                {
                    "code": str(row["code"]),
                    "rank": int(row.get("rank") or 0),
                    "entry": float(entry),
                    "stop": float(stop),
                    "target": float(target),
                }
            )

        if not triggered_today:
            equity_rows.append((str(nd.date()), equity))
            continue

        triggered_today = sorted(triggered_today, key=lambda x: x["rank"])[: max_orders_per_day]
        trigger_count += int(len(triggered_today))

        sizing_base = equity if use_dynamic_sizing else float(initial_cash)
        alloc_cash = sizing_base * alloc_pct

        day_pnl = 0.0
        for t in triggered_today:
            code = t["code"]
            if code not in next_map.index:
                continue

            nr = next_map.loc[code]
            low_n = float(nr["low"])
            high_n = float(nr["high"])
            close_n = float(nr["close"])

            entry_px = float(t["entry"])
            stop_px = float(t["stop"])
            target_px = float(t["target"])

            if low_n > entry_px:
                continue

            qty = int(alloc_cash // entry_px) if alloc_cash > 0 else 0
            if qty <= 0:
                continue

            hit_stop = low_n <= stop_px
            hit_target = high_n >= target_px
            if both_hit_rule == "target_first":
                exit_px = target_px if hit_target else (stop_px if hit_stop else close_n)
            else:
                exit_px = stop_px if hit_stop else (target_px if hit_target else close_n)

            entry_net = entry_px * entry_fee_mult
            exit_net = exit_px * exit_fee_mult
            pnl = qty * (exit_net - entry_net)

            trade_pnls.append(float(pnl))
            day_pnl += float(pnl)

        equity += day_pnl
        equity_rows.append((str(nd.date()), equity))

    eq = pd.DataFrame(equity_rows, columns=["date", "equity"])
    eq["equity"] = eq["equity"].astype(float)
    dd = (eq["equity"] / eq["equity"].cummax()) - 1.0

    wins = sum(1 for p in trade_pnls if p > 0)
    trades = len(trade_pnls)
    win_rate = (wins / trades) if trades else 0.0
    total_return = (float(eq["equity"].iloc[-1]) / float(initial_cash)) - 1.0 if initial_cash else 0.0

    return YearMetrics(
        year=int(year_start.year),
        start_date=str(year_start.date()),
        end_date=str(year_end.date()),
        n_days=int(len(dates)),
        selection_count=int(selection_count),
        trigger_count=int(trigger_count),
        trade_count=int(trades),
        win_rate=float(win_rate),
        total_return=float(total_return),
        mdd=float(dd.min()) if len(dd) else 0.0,
        pnl=float(sum(trade_pnls)),
        end_equity=float(eq["equity"].iloc[-1]) if not eq.empty else float(initial_cash),
    )


def _format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def main() -> None:
    parser = argparse.ArgumentParser(description="Yearly backtest for TraderUS selection + daytrade config.")
    parser.add_argument("--db", default="data/market_data.db")
    parser.add_argument("--from-year", type=int, default=2021)
    parser.add_argument("--to-year", type=int, default=None, help="Default: max year in DB")
    parser.add_argument("--output-dir", default="data/backtest_daytrade_yearly")
    parser.add_argument("--sizing-mode", choices=["fixed", "dynamic"], default="fixed")
    parser.add_argument("--initial-cash", type=float, default=None, help="Default: strategy.position.initial_cash")
    args = parser.parse_args()

    settings = load_settings()
    params = load_strategy(settings)
    daytrade_cfg = load_daytrade_cfg(settings)
    if not bool(daytrade_cfg.get("enabled", False)):
        raise SystemExit("daytrade.enabled is false in strategy config.")

    sig = daytrade_cfg.get("signal") or {}
    br = daytrade_cfg.get("bracket") or {}
    ex = daytrade_cfg.get("execution") or {}

    rsi_period = int(sig.get("rsi_period", 2))
    rsi_thresh = float(sig.get("rsi_thresh", 10))
    sma_fast_n = int(sig.get("sma_fast", 5))
    use_daytrade_trend = bool(sig.get("use_trend_filter", False))
    sma_trend_n = int(sig.get("sma_trend", 200))
    atr_n = int(sig.get("atr_period", 14))

    entry_k = float(br.get("entry_k", 1.0))
    stop_mult = float(br.get("stop_mult", 1.75))
    target_mult = float(br.get("target_mult", 0.75))
    both_hit_rule = str(br.get("both_hit_rule", "stop_first"))

    max_orders_per_day = int(ex.get("max_orders_per_day", 5))
    alloc_pct = float(ex.get("alloc_pct", 0.2))
    fee_bps = float(ex.get("fee_bps", 20))
    min_atr_pct = float(ex.get("min_atr_pct", 0.0) or 0.0)

    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    liquidity_rank = int(getattr(params, "liquidity_rank", 300))
    min_amount = float(getattr(params, "min_amount", 0))
    buy_nasdaq = float(getattr(params, "buy_kospi", -0.05))
    buy_sp500 = float(getattr(params, "buy_kosdaq", -0.06))
    trend_ma25_rising = bool(getattr(params, "trend_ma25_rising", False))
    max_positions = int(getattr(params, "max_positions", 20))
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)

    initial_cash = float(args.initial_cash) if args.initial_cash is not None else float(getattr(params, "initial_cash", 100000))

    store = SQLiteStore(args.db)
    row = store.conn.execute("SELECT MIN(date), MAX(date) FROM daily_price").fetchone()
    if not row or not row[1]:
        raise SystemExit("daily_price has no rows")
    min_date = pd.to_datetime(row[0])
    max_date = pd.to_datetime(row[1])

    to_year = int(args.to_year) if args.to_year is not None else int(max_date.year)
    from_year = int(args.from_year)
    if from_year > to_year:
        raise SystemExit("from-year must be <= to-year")

    warmup_days = max(250, sma_trend_n + 20 if use_daytrade_trend else 60)
    query_start = max(min_date, pd.Timestamp(datetime(from_year, 1, 1)) - pd.Timedelta(days=warmup_days))
    query_end = min(max_date, pd.Timestamp(datetime(to_year, 12, 31)))

    prices = _load_prices_with_universe(
        store.conn,
        start_date=str(query_start.date()),
        end_date=str(query_end.date()),
    )
    if prices.empty:
        raise SystemExit("No data loaded for the selected range.")

    prices = _compute_indicators(
        prices,
        sma_fast_n=sma_fast_n,
        atr_n=atr_n,
        rsi_n=rsi_period,
        sma_trend_n=sma_trend_n,
        use_trend_filter=use_daytrade_trend,
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[YearMetrics] = []
    for y in range(from_year, to_year + 1):
        y_start = pd.Timestamp(datetime(y, 1, 1))
        y_end = pd.Timestamp(datetime(y, 12, 31))
        if y == int(max_date.year):
            y_end = min(y_end, max_date)
        if y_end < y_start:
            continue

        m = _simulate_one_year(
            prices,
            year_start=y_start,
            year_end=y_end,
            initial_cash=initial_cash,
            use_dynamic_sizing=(args.sizing_mode == "dynamic"),
            entry_mode=entry_mode,
            rank_mode=rank_mode,
            min_amount=min_amount,
            liquidity_rank=liquidity_rank,
            buy_nasdaq=buy_nasdaq,
            buy_sp500=buy_sp500,
            trend_ma25_rising=trend_ma25_rising,
            max_positions=max_positions,
            max_per_sector=max_per_sector,
            rsi_thresh=rsi_thresh,
            entry_k=entry_k,
            stop_mult=stop_mult,
            target_mult=target_mult,
            use_daytrade_trend=use_daytrade_trend,
            min_atr_pct=min_atr_pct,
            max_orders_per_day=max_orders_per_day,
            fee_bps=fee_bps,
            both_hit_rule=both_hit_rule,
            alloc_pct=alloc_pct,
        )
        rows.append(m)
        print(
            f"{m.year} | trades={m.trade_count} win={_format_pct(m.win_rate)} "
            f"ret={_format_pct(m.total_return)} mdd={_format_pct(m.mdd)}"
        )

    out_df = pd.DataFrame([r.__dict__ for r in rows])
    out_csv = out_dir / f"yearly_summary_{args.sizing_mode}.csv"
    out_df.to_csv(out_csv, index=False)
    print(f"saved {out_csv}")


if __name__ == "__main__":
    main()
