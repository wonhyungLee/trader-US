from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.autotrade.info_loader import AutoTradeInfo, load_autotrade_info
from src.autotrade.payloads import build_limit_order, infer_exchange, infer_quote_currency
from src.autotrade.price_feed import fetch_current_price_us
from src.analyzer.backtest_runner import load_strategy
from src.daytrade.planner import (
    build_orders_from_plans,
    build_traderus_selection,
    compute_plan_for_code,
    load_daytrade_cfg,
    next_business_day,
    resolve_total_assets,
)
from src.storage.sqlite_store import SQLiteStore, normalize_code
from src.utils.config import load_settings
from src.utils.notifier import maybe_notify
from src.utils.project_root import ensure_repo_root


LIST_SELECTED = "SELECTED"
LIST_EXIT = "EXIT"


@dataclass(frozen=True)
class AutoTradeConfig:
    enabled: bool
    send_enabled: bool
    db_path: str
    info_path: str
    webhook_url: str
    password: str
    kis_number: str
    default_amount: int
    poll_interval_sec: int
    optimize: bool
    optimize_lookback_bars: Optional[int]
    sync_selection_enabled: bool
    sync_selection_interval_sec: int
    sync_selection_max_codes: int
    cancel_missing_selected: bool
    generate_sell_queue: bool
    purge_expired_after_days: int


def utc_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def load_autotrade_config(settings: dict) -> AutoTradeConfig:
    cfg = settings.get("autotrade", {}) or {}
    enabled = bool(cfg.get("enabled", False))
    send_enabled = bool(cfg.get("send_enabled", False))
    db_path = str((settings.get("database", {}) or {}).get("path", "data/market_data.db"))
    info_path = str(cfg.get("info_path") or "").strip()
    webhook_url = str(cfg.get("webhook_url") or "").strip()
    password = str(cfg.get("password") or "").strip()
    kis_number = str(cfg.get("kis_number") or "").strip()
    default_amount = int(cfg.get("default_amount") or 1)
    poll_interval_sec = int(cfg.get("poll_interval_sec") or 60)
    optimize = bool(cfg.get("optimize", True))
    lookback_raw = cfg.get("optimize_lookback_bars")
    try:
        optimize_lookback_bars = int(lookback_raw) if lookback_raw is not None else None
    except Exception:
        optimize_lookback_bars = None
    sync_selection_enabled = bool(cfg.get("sync_selection_enabled", True))
    sync_selection_interval_sec = int(cfg.get("sync_selection_interval_sec") or 300)
    sync_selection_max_codes = int(
        cfg.get("sync_selection_max_codes")
        or (settings.get("strategy", {}) or {}).get("max_positions")
        or 20
    )
    cancel_missing_selected = bool(cfg.get("cancel_missing_selected", True))
    generate_sell_queue = bool(cfg.get("generate_sell_queue", False))
    purge_expired_after_days = int(cfg.get("purge_expired_after_days") or 7)

    if info_path:
        info = load_autotrade_info(info_path)
        if not webhook_url and info.webhook_url:
            webhook_url = info.webhook_url
        if _is_placeholder(password) and info.password:
            password = info.password
        if _is_placeholder(kis_number) and info.kis_number:
            kis_number = info.kis_number

    return AutoTradeConfig(
        enabled=enabled,
        send_enabled=send_enabled,
        db_path=db_path,
        info_path=info_path,
        webhook_url=webhook_url,
        password=password,
        kis_number=kis_number,
        default_amount=max(1, default_amount),
        poll_interval_sec=max(10, poll_interval_sec),
        optimize=optimize,
        optimize_lookback_bars=optimize_lookback_bars,
        sync_selection_enabled=sync_selection_enabled,
        sync_selection_interval_sec=max(30, sync_selection_interval_sec),
        sync_selection_max_codes=max(1, sync_selection_max_codes),
        cancel_missing_selected=cancel_missing_selected,
        generate_sell_queue=generate_sell_queue,
        purge_expired_after_days=max(0, purge_expired_after_days),
    )


def _is_placeholder(value: str) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.startswith("${") and text.endswith("}")


def _enrich_symbol(store: SQLiteStore, code: str) -> Tuple[str, str, str]:
    """Return (name, market, excd) for the code using watchlist/universe members."""
    code = normalize_code(code)
    row = store.conn.execute(
        "SELECT name, market, excd FROM autotrade_watchlist WHERE code=?",
        (code,),
    ).fetchone()
    name = (row[0] if row else "") or ""
    market = (row[1] if row else "") or ""
    excd = (row[2] if row else "") or ""
    if name and excd:
        return (str(name), str(market), str(excd))

    u = store.conn.execute(
        "SELECT name, market, excd FROM universe_members WHERE code=?",
        (code,),
    ).fetchone()
    if u:
        name = name or (u[0] or "")
        market = market or (u[1] or "")
        excd = excd or (u[2] or "")

    return (str(name or ""), str(market or ""), str(excd or ""))


_SELECTION_SYNC_STATE: Dict[str, Any] = {"last_ts": 0.0, "last_price_date": ""}


def _latest_price_date(store: SQLiteStore) -> str:
    row = store.conn.execute("SELECT MAX(date) FROM daily_price").fetchone()
    if row and row[0]:
        return str(row[0])
    return ""


def _selected_codes_from_strategy(store: SQLiteStore, settings: dict, limit: int) -> Optional[List[str]]:
    try:
        params = load_strategy(settings)
    except Exception:
        logging.exception("[autotrade] failed to load strategy params")
        return None

    min_amount = float(getattr(params, "min_amount", 0) or 0)
    liquidity_rank = int(getattr(params, "liquidity_rank", 0) or 0)
    buy_nasdaq = float(getattr(params, "buy_kospi", 0) or 0)
    buy_sp500 = float(getattr(params, "buy_kosdaq", 0) or 0)
    max_positions = int(getattr(params, "max_positions", 20) or 20)
    max_per_sector = int(getattr(params, "max_per_sector", 0) or 0)
    entry_mode = str(getattr(params, "entry_mode", "mean_reversion") or "mean_reversion").lower()
    rank_mode = str(getattr(params, "rank_mode", "amount") or "amount").lower()
    trend_filter = bool(getattr(params, "trend_ma25_rising", False))
    max_pick = max(1, min(int(limit), max_positions))

    sql = """
    SELECT dp.code,
           COALESCE(u.market, '') AS market,
           COALESCE(u.group_name, 'UNKNOWN') AS group_name,
           COALESCE(sm.sector_name, u.group_name, 'UNKNOWN') AS sector_name,
           CAST(COALESCE(dp.amount, 0) AS REAL) AS amount,
           CAST(COALESCE(dp.disparity, 0) AS REAL) AS disparity,
           CAST(COALESCE(dp.ma25, 0) AS REAL) AS ma25,
           CAST(COALESCE(prev.ma25, 0) AS REAL) AS ma25_prev,
           CAST(COALESCE(dp.close, 0) AS REAL) AS close,
           CAST(COALESCE(prev3.close, 0) AS REAL) AS close_prev3
    FROM daily_price dp
    JOIN (
      SELECT code, MAX(date) AS max_date
      FROM daily_price
      GROUP BY code
    ) mx
      ON dp.code = mx.code
     AND dp.date = mx.max_date
    JOIN universe_members u
      ON dp.code = u.code
    LEFT JOIN sector_map sm
      ON sm.code = dp.code
    LEFT JOIN daily_price prev
      ON prev.code = dp.code
     AND prev.date = (
       SELECT p2.date
       FROM daily_price p2
       WHERE p2.code = dp.code
         AND p2.date < dp.date
       ORDER BY p2.date DESC
       LIMIT 1
     )
    LEFT JOIN daily_price prev3
      ON prev3.code = dp.code
     AND prev3.date = (
       SELECT p4.date
       FROM daily_price p4
       WHERE p4.code = dp.code
         AND p4.date < dp.date
       ORDER BY p4.date DESC
       LIMIT 1 OFFSET 2
     )
    ORDER BY amount DESC
    """
    rows = store.conn.execute(sql).fetchall()
    if not rows:
        return []

    liquid_rows = []
    for r in rows:
        amount = float(r[4] or 0)
        if amount < min_amount:
            continue
        liquid_rows.append(r)
        if liquidity_rank > 0 and len(liquid_rows) >= liquidity_rank:
            break

    candidates: List[Dict[str, Any]] = []
    for r in liquid_rows:
        code = normalize_code(r[0])
        group_hint = str(r[2] or r[1] or "").upper()
        sector = str(r[3] or "UNKNOWN")
        amount = float(r[4] or 0)
        disparity = float(r[5] or 0)
        ma25 = float(r[6] or 0)
        ma25_prev = float(r[7] or 0)
        close = float(r[8] or 0)
        close_prev3 = float(r[9] or 0)
        ret3 = ((close / close_prev3) - 1.0) if close_prev3 > 0 else 0.0

        buy_th = buy_nasdaq if "NASDAQ" in group_hint else buy_sp500
        if entry_mode == "trend_follow":
            if not (disparity >= buy_th and ret3 >= 0):
                continue
        elif disparity > buy_th:
            continue

        if trend_filter and not (ma25 > ma25_prev):
            continue

        score: Optional[float] = None
        if rank_mode == "score":
            if entry_mode == "trend_follow":
                score = disparity + (0.8 * ret3) + (0.05 * math.log1p(max(amount, 0.0)))
            else:
                score = (-disparity) + (0.8 * (-ret3)) + (0.05 * math.log1p(max(amount, 0.0)))

        candidates.append(
            {
                "code": code,
                "sector": sector,
                "amount": amount,
                "score": score,
            }
        )

    if rank_mode == "score":
        candidates.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    else:
        candidates.sort(key=lambda x: float(x.get("amount") or 0.0), reverse=True)

    sector_counts: Dict[str, int] = {}
    try:
        held_rows = store.conn.execute(
            """
            SELECT p.code,
                   COALESCE(sm.sector_name, u.group_name, 'UNKNOWN') AS sector_name
            FROM position_state p
            LEFT JOIN sector_map sm ON p.code = sm.code
            LEFT JOIN universe_members u ON p.code = u.code
            """
        ).fetchall()
        for h in held_rows:
            sec = str((h[1] if h else "") or "UNKNOWN")
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
    except Exception:
        sector_counts = {}

    selected: List[str] = []
    for cand in candidates:
        sector = str(cand.get("sector") or "UNKNOWN")
        if max_per_sector and sector_counts.get(sector, 0) >= max_per_sector:
            continue
        code = str(cand.get("code") or "").upper()
        if not code:
            continue
        selected.append(code)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected) >= max_pick:
            break
    return selected


def _sync_selected_watchlist(store: SQLiteStore, settings: dict, cfg: AutoTradeConfig) -> Optional[set[str]]:
    if not cfg.sync_selection_enabled:
        return None

    now_ts = time.time()
    latest_date = _latest_price_date(store)
    state = _SELECTION_SYNC_STATE
    # Sync when price date changes or fixed interval elapsed.
    if (
        state.get("last_price_date") == latest_date
        and (now_ts - float(state.get("last_ts", 0.0))) < float(cfg.sync_selection_interval_sec)
    ):
        return None

    selected_codes = _selected_codes_from_strategy(store, settings, cfg.sync_selection_max_codes)
    if selected_codes is None:
        return None

    rows = store.conn.execute(
        "SELECT code, list_type, enabled FROM autotrade_watchlist"
    ).fetchall()
    row_map = {
        normalize_code(r[0]): {"list_type": str(r[1] or "").upper(), "enabled": int(r[2] or 0)}
        for r in rows
        if r and r[0]
    }

    managed_selected: set[str] = set()
    skipped_exit = 0
    for code in selected_codes:
        cur = row_map.get(code)
        if cur and cur["list_type"] == LIST_EXIT and cur["enabled"] == 1:
            # Respect explicit EXIT rows set by user.
            skipped_exit += 1
            continue
        name, market, excd = _enrich_symbol(store, code)
        store.upsert_autotrade_watchlist(
            code,
            name=name,
            market=market,
            excd=excd,
            list_type=LIST_SELECTED,
            enabled=True,
        )
        managed_selected.add(code)

    to_disable = []
    desired_set = set(selected_codes)
    for code, rec in row_map.items():
        if rec["list_type"] != LIST_SELECTED:
            continue
        if rec["enabled"] != 1:
            continue
        if code in desired_set:
            continue
        to_disable.append(code)

    if to_disable:
        now = datetime.utcnow().isoformat()
        for code in to_disable:
            store.conn.execute(
                "UPDATE autotrade_watchlist SET enabled=0, updated_at=? WHERE code=? AND list_type=?",
                (now, code, LIST_SELECTED),
            )
        store.conn.commit()

    state["last_ts"] = now_ts
    state["last_price_date"] = latest_date
    logging.info(
        "[autotrade] selection sync managed=%s disabled=%s skipped_exit=%s latest_date=%s",
        len(managed_selected),
        len(to_disable),
        skipped_exit,
        latest_date or "-",
    )
    return managed_selected


def _cancel_pending_sells(store: SQLiteStore, reason: str) -> int:
    now = utc_ts()
    cur = store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status='CANCELLED', last_error=?, updated_at=?
        WHERE side='SELL' AND status IN ('PENDING','ERROR','SKIPPED')
        """,
        (reason, now),
    )
    store.conn.commit()
    return int(cur.rowcount or 0)


def _cancel_missing_selected_buys(store: SQLiteStore, desired_buy_codes: set[str], reason: str) -> int:
    now = utc_ts()
    if desired_buy_codes:
        placeholders = ",".join("?" * len(desired_buy_codes))
        params: Tuple[Any, ...] = (reason, now, *sorted(desired_buy_codes))
        sql = (
            "UPDATE autotrade_queue "
            "SET status='CANCELLED', last_error=?, updated_at=? "
            "WHERE side='BUY' AND status IN ('PENDING','ERROR','SKIPPED') "
            f"AND code NOT IN ({placeholders})"
        )
        cur = store.conn.execute(sql, params)
    else:
        cur = store.conn.execute(
            """
            UPDATE autotrade_queue
            SET status='CANCELLED', last_error=?, updated_at=?
            WHERE side='BUY' AND status IN ('PENDING','ERROR','SKIPPED')
            """,
            (reason, now),
        )
    store.conn.commit()
    return int(cur.rowcount or 0)


def _expire_and_purge_queue(store: SQLiteStore, cfg: AutoTradeConfig) -> Tuple[int, int]:
    now = utc_ts()
    # Use latest loaded market date instead of UTC day rollover.
    # This avoids expiring US queues too early before next daily bar is available.
    expire_before = _latest_price_date(store) or datetime.utcnow().date().isoformat()
    expired_cur = store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status='EXPIRED',
            last_error=CASE WHEN COALESCE(last_error,'')='' THEN 'expired_past_asof' ELSE last_error END,
            updated_at=?
        WHERE status IN ('PENDING','ERROR','SKIPPED') AND asof_date < ?
        """,
        (now, expire_before),
    )

    cutoff = (datetime.utcnow().date() - timedelta(days=int(cfg.purge_expired_after_days))).isoformat()
    purged_cur = store.conn.execute(
        """
        DELETE FROM autotrade_queue
        WHERE status IN ('EXPIRED','CANCELLED') AND asof_date < ?
        """,
        (cutoff,),
    )
    store.conn.commit()
    return int(expired_cur.rowcount or 0), int(purged_cur.rowcount or 0)


def _sync_daytrade_plans_and_queue(
    store: SQLiteStore,
    cfg: AutoTradeConfig,
    settings: dict,
    managed_selected: Optional[set[str]],
) -> Dict[str, Any]:
    """Generate queue from TraderUS selection + daytrade(balanced) planner."""
    daytrade_cfg = load_daytrade_cfg(settings)
    if not bool(daytrade_cfg.get("enabled", False)):
        return {"ok": False, "reason": "daytrade_disabled"}

    signal_date = _latest_price_date(store)
    if not signal_date:
        return {"ok": False, "reason": "no_price_data"}

    latest_sel_date, sel_df = build_traderus_selection(store.conn, settings)
    if sel_df.empty:
        cancelled = _cancel_missing_selected_buys(store, set(), "no_daytrade_candidates")
        return {"ok": True, "signal_date": signal_date, "orders_count": 0, "cancelled": cancelled}
    if latest_sel_date and latest_sel_date != signal_date:
        logging.warning(
            "[autotrade] selection latest date differs: selection=%s daily=%s",
            latest_sel_date,
            signal_date,
        )

    if managed_selected is not None:
        allowed_codes = set(managed_selected)
    else:
        wl_rows = store.list_autotrade_watchlist(list_type=LIST_SELECTED, enabled_only=True)
        allowed_codes = {
            normalize_code(r["code"])
            for r in wl_rows
            if r and r["code"]
        }
        if not allowed_codes:
            allowed_codes = {
                normalize_code(c)
                for c in sel_df["code"].astype(str).tolist()
            }

    ex = daytrade_cfg.get("execution") or {}
    max_orders = max(1, int(ex.get("max_orders_per_day", 5)))
    min_atr_pct = float(ex.get("min_atr_pct", 0.0) or 0.0)

    plans = []
    for _, row in sel_df.iterrows():
        code = normalize_code(row.get("code"))
        if code not in allowed_codes:
            continue
        rank = int(row.get("rank") or 0)
        plan = compute_plan_for_code(
            store.conn,
            code=code,
            rank=rank,
            signal_date=signal_date,
            daytrade_cfg=daytrade_cfg,
            enforce_signal_trigger=False,
            fallback_to_latest_before_signal_date=True,
        )
        if not plan:
            continue
        if (min_atr_pct > 0) and (float(plan.atr_pct) < min_atr_pct):
            continue
        plans.append(plan)

    plans.sort(key=lambda p: p.rank)
    plans = plans[:max_orders]

    total_assets = resolve_total_assets(settings)
    exec_date = next_business_day(signal_date)
    planner_orders = build_orders_from_plans(
        plans,
        daytrade_cfg=daytrade_cfg,
        total_assets=total_assets,
        exec_date=exec_date,
        signal_date=signal_date,
    )

    info = AutoTradeInfo(webhook_url=cfg.webhook_url, password=cfg.password, kis_number=cfg.kis_number)
    if not info.webhook_url or not info.password or not info.kis_number:
        logging.warning(
            "[autotrade] missing webhook config (url/password/kis_number). url=%s",
            bool(info.webhook_url),
        )
        return {
            "ok": False,
            "reason": "webhook_config_missing",
            "signal_date": signal_date,
            "orders_count": len(planner_orders),
        }

    planned_codes: set[str] = set()
    for od in planner_orders:
        code = normalize_code(od.get("code"))
        if not code:
            continue
        entry_price = _to_float(od.get("ord_unpr"))
        if entry_price is None or entry_price <= 0:
            continue
        target_price = _to_float(od.get("target_unpr"))
        stop_price = _to_float(od.get("stop_unpr"))
        qty = int(od.get("qty") or cfg.default_amount or 1)
        if qty <= 0:
            qty = max(1, int(cfg.default_amount))

        store.upsert_autotrade_plan(
            asof_date=signal_date,
            code=code,
            entry_price=entry_price,
            target_price=target_price,
            stop_price=stop_price,
            confidence=None,
            status="READY",
            plan_json=str(od.get("meta_json") or "{}"),
        )

        name, market, excd = _enrich_symbol(store, code)
        exchange = infer_exchange(excd, code)
        quote = infer_quote_currency(code)
        order_name = f"{name or code} 매매"

        payload = build_limit_order(
            password=info.password,
            exchange=exchange,
            base=code,
            quote=quote,
            side="buy",
            amount=qty,
            price=float(entry_price),
            order_name=order_name,
            kis_number=info.kis_number,
        )
        existing_buy = store.conn.execute(
            "SELECT status FROM autotrade_queue WHERE asof_date=? AND code=? AND side='BUY'",
            (signal_date, code),
        ).fetchone()
        if existing_buy and str(existing_buy[0] or "").upper() in {"SENT", "SENDING"}:
            planned_codes.add(code)
            continue
        store.upsert_autotrade_queue(
            asof_date=signal_date,
            code=code,
            side="BUY",
            trigger_price=float(payload.get("price") or 0),
            trigger_rule="<=",
            webhook_url=info.webhook_url,
            payload_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
            status="PENDING",
        )

        if cfg.generate_sell_queue and target_price is not None:
            sell_payload = build_limit_order(
                password=info.password,
                exchange=exchange,
                base=code,
                quote=quote,
                side="sell",
                amount=qty,
                price=float(target_price),
                order_name=order_name,
                kis_number=info.kis_number,
            )
            existing_sell = store.conn.execute(
                "SELECT status FROM autotrade_queue WHERE asof_date=? AND code=? AND side='SELL'",
                (signal_date, code),
            ).fetchone()
            if existing_sell and str(existing_sell[0] or "").upper() in {"SENT", "SENDING"}:
                continue
            store.upsert_autotrade_queue(
                asof_date=signal_date,
                code=code,
                side="SELL",
                trigger_price=float(sell_payload.get("price") or 0),
                trigger_rule=">=",
                webhook_url=info.webhook_url,
                payload_json=json.dumps(sell_payload, ensure_ascii=False, separators=(",", ":"), default=str),
                status="PENDING",
            )

        # Keep metadata in watchlist for UI.
        list_type_row = store.conn.execute(
            "SELECT list_type FROM autotrade_watchlist WHERE code=?",
            (code,),
        ).fetchone()
        list_type = str((list_type_row[0] if list_type_row else LIST_SELECTED) or LIST_SELECTED).upper()
        store.upsert_autotrade_watchlist(
            code,
            name=name,
            market=market,
            excd=excd,
            list_type=list_type,
            enabled=True,
        )
        planned_codes.add(code)

    cancelled = 0
    if cfg.cancel_missing_selected:
        cancelled = _cancel_missing_selected_buys(store, planned_codes, "cancelled_not_in_daytrade_plan")
        if cancelled:
            logging.info("[autotrade] cancelled BUY queue not in daytrade plans=%s", cancelled)

    return {
        "ok": True,
        "signal_date": signal_date,
        "exec_date": exec_date,
        "orders_count": len(planned_codes),
        "cancelled": cancelled,
    }


def _latest_queue_asof_per_code(store: SQLiteStore) -> Dict[str, str]:
    rows = store.conn.execute("SELECT code, MAX(asof_date) FROM autotrade_queue GROUP BY code").fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        if r and r[0] and r[1]:
            out[str(r[0]).upper()] = str(r[1])
    return out


def _list_dispatch_candidates(store: SQLiteStore) -> List[Dict[str, Any]]:
    # Only consider latest asof_date per code to avoid stale PENDINGs.
    sql = """
    WITH latest AS (
      SELECT code, MAX(asof_date) AS max_asof
      FROM autotrade_queue
      GROUP BY code
    )
    SELECT q.id, q.asof_date, q.code, q.side, q.trigger_price, q.trigger_rule,
           q.webhook_url, q.payload_json, q.status,
           w.list_type, w.enabled, w.name, w.market, w.excd
    FROM autotrade_queue q
    JOIN latest l
      ON q.code = l.code AND q.asof_date = l.max_asof
    JOIN autotrade_watchlist w
      ON q.code = w.code
    WHERE q.status='PENDING' AND w.enabled=1
    ORDER BY q.asof_date DESC, q.code ASC, q.side ASC, q.id ASC
    """
    rows = store.conn.execute(sql).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "asof_date": r[1],
                "code": str(r[2]).upper(),
                "side": str(r[3]).upper(),
                "trigger_price": r[4],
                "trigger_rule": r[5],
                "webhook_url": r[6],
                "payload_json": r[7],
                "status": r[8],
                "list_type": str(r[9]).upper() if r[9] else "",
                "enabled": int(r[10] or 0),
                "name": r[11] or "",
                "market": r[12] or "",
                "excd": r[13] or "",
            }
        )
    return out


def _should_dispatch(list_type: str, side: str) -> bool:
    side = str(side or "").upper()
    list_type = str(list_type or "").upper()
    if side == "BUY":
        return list_type == LIST_SELECTED
    if side == "SELL":
        return list_type == LIST_EXIT
    return False


def _trigger_met(rule: str, price: float, trigger_price: float) -> bool:
    if rule == "<=":
        return price <= trigger_price
    if rule == ">=":
        return price >= trigger_price
    return False


def _claim_pending(store: SQLiteStore, order_id: int) -> bool:
    now = utc_ts()
    cur = store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status='SENDING',
            attempt_count=COALESCE(attempt_count, 0) + 1,
            last_attempt_at=?,
            updated_at=?
        WHERE id=? AND status='PENDING'
        """,
        (now, now, int(order_id)),
    )
    store.conn.commit()
    return int(cur.rowcount or 0) == 1


def _mark_result(
    store: SQLiteStore,
    *,
    order_id: int,
    status: str,
    http_status: Optional[int] = None,
    response_text: str = "",
    error_text: str = "",
) -> None:
    now = utc_ts()
    store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status=?,
            sent_at=CASE WHEN ?='SENT' THEN ? ELSE sent_at END,
            response_text=?,
            last_error=?,
            updated_at=?
        WHERE id=?
        """,
        (
            str(status).upper(),
            str(status).upper(),
            now,
            (response_text or "")[:8000],
            (error_text or "")[:8000],
            now,
            int(order_id),
        ),
    )
    store.conn.commit()

    # Best-effort event log
    row = store.conn.execute(
        "SELECT asof_date, code, side, webhook_url, payload_json FROM autotrade_queue WHERE id=?",
        (int(order_id),),
    ).fetchone()
    if row:
        store.insert_autotrade_event(
            ts=now,
            asof_date=str(row[0]),
            code=str(row[1]),
            side=str(row[2]),
            status=str(status).upper(),
            http_status=http_status,
            webhook_url=str(row[3] or ""),
            payload_json=str(row[4] or ""),
            response_text=response_text or "",
            error_text=error_text or "",
        )


def _send_webhook(url: str, payload: Dict[str, Any]) -> Tuple[bool, Optional[int], str]:
    resp = requests.post(url, json=payload, timeout=(4, 12))
    ok = bool(resp.ok)
    body = (resp.text or "").strip()
    return ok, int(resp.status_code), body


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _fetch_current_prices_kr(settings: dict, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch KR current prices via KIS multi-price (best-effort)."""
    out: Dict[str, Dict[str, Any]] = {}
    if not codes:
        return out
    try:
        from src.brokers.kis_broker import KISBroker
        broker = KISBroker(settings)
    except Exception as exc:
        logging.warning("[autotrade] KISBroker unavailable: %s", exc)
        return out

    now = utc_ts()
    # KIS multi endpoint supports up to 30 symbols.
    for i in range(0, len(codes), 30):
        batch = [str(c).strip().zfill(6) for c in codes[i : i + 30] if str(c).strip()]
        if not batch:
            continue
        try:
            res = broker.get_multi_price(batch)
            outputs = res.get("output") or res.get("output1") or []
            if not isinstance(outputs, list):
                continue
            for rec in outputs:
                code = rec.get("inter_shrn_iscd") or rec.get("stck_shrn_iscd") or rec.get("iscd") or rec.get("code")
                if not code:
                    continue
                code = str(code).strip().zfill(6)
                price = rec.get("inter2_prpr") or rec.get("stck_prpr") or rec.get("prpr") or rec.get("price")
                out[code] = {"code": code, "price": _to_float(price), "asof": now, "source": "kis"}
        except Exception as exc:
            logging.warning("[autotrade] KR price batch failed (%s..): %s", batch[0], exc)
    return out


def _resolve_us_excd_map(store: SQLiteStore, codes: List[str]) -> Dict[str, str]:
    targets = [normalize_code(c) for c in codes if str(c).strip()]
    if not targets:
        return {}
    out: Dict[str, str] = {c: "" for c in targets}

    placeholders = ",".join(["?"] * len(targets))
    wl_rows = store.conn.execute(
        f"SELECT code, COALESCE(excd, '') FROM autotrade_watchlist WHERE code IN ({placeholders})",
        tuple(targets),
    ).fetchall()
    for row in wl_rows:
        code = normalize_code(row[0])
        excd = str(row[1] or "").strip().upper()
        if code in out and excd:
            out[code] = excd

    missing = [c for c, e in out.items() if not e]
    if not missing:
        return out

    placeholders2 = ",".join(["?"] * len(missing))
    uni_rows = store.conn.execute(
        f"SELECT code, COALESCE(excd, '') FROM universe_members WHERE code IN ({placeholders2})",
        tuple(missing),
    ).fetchall()
    for row in uni_rows:
        code = normalize_code(row[0])
        excd = str(row[1] or "").strip().upper()
        if code in out and excd:
            out[code] = excd

    return out


def _fetch_current_prices_us(store: SQLiteStore, settings: dict, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not codes:
        return out

    code_excd = _resolve_us_excd_map(store, codes)
    try:
        from src.brokers.kis_broker import KISBroker

        broker = KISBroker(settings)
    except Exception as exc:
        logging.warning("[autotrade] KISBroker unavailable for US quotes: %s", exc)
        for code in codes:
            out[code] = {"code": code, "price": None, "asof": None, "source": "kis"}
        return out

    for code in codes:
        out[code] = fetch_current_price_us(
            code,
            settings=settings,
            excd=code_excd.get(code),
            broker=broker,
            allow_fallback=False,
        )
    return out


def run_cycle(store: SQLiteStore, settings: dict, cfg: AutoTradeConfig, *, dry_run: bool = False) -> None:
    if not cfg.enabled:
        return

    expired, purged = _expire_and_purge_queue(store, cfg)
    if expired or purged:
        logging.info("[autotrade] queue maintenance expired=%s purged=%s", expired, purged)

    managed_selected = _sync_selected_watchlist(store, settings, cfg)
    if managed_selected is not None and cfg.cancel_missing_selected:
        cancelled = _cancel_missing_selected_buys(store, managed_selected, "cancelled_missing_selection")
        if cancelled:
            logging.info("[autotrade] cancelled missing BUY queue=%s", cancelled)

    if not cfg.generate_sell_queue:
        cancelled_sell = _cancel_pending_sells(store, "sell_queue_disabled")
        if cancelled_sell:
            logging.info("[autotrade] cancelled pending SELL queue=%s", cancelled_sell)

    sync_res = _sync_daytrade_plans_and_queue(store, cfg, settings, managed_selected)
    if not sync_res.get("ok"):
        logging.info("[autotrade] daytrade queue sync skipped: %s", sync_res)

    candidates = _list_dispatch_candidates(store)
    if not candidates:
        return

    # Price fetch once per code
    by_code: Dict[str, List[Dict[str, Any]]] = {}
    for ev in candidates:
        by_code.setdefault(ev["code"], []).append(ev)

    all_codes = list(by_code.keys())
    kr_codes = [c for c in all_codes if str(c).isdigit()]
    us_codes = [c for c in all_codes if not str(c).isdigit()]
    price_map: Dict[str, Dict[str, Any]] = {}
    if kr_codes:
        price_map.update(_fetch_current_prices_kr(settings, kr_codes))
    if us_codes:
        price_map.update(_fetch_current_prices_us(store, settings, us_codes))

    for code, items in by_code.items():
        price_rec = price_map.get(code) or {}
        price = price_rec.get("price")
        if price is None:
            logging.warning("[autotrade] skip dispatch %s: current price unavailable", code)
            continue

        for it in items:
            if str(it.get("side") or "").upper() == "SELL" and not cfg.generate_sell_queue:
                continue
            if not _should_dispatch(it.get("list_type", ""), it.get("side", "")):
                continue
            tp = it.get("trigger_price")
            if tp is None:
                continue
            try:
                tp_f = float(tp)
                price_f = float(price)
            except Exception:
                continue
            if not _trigger_met(str(it.get("trigger_rule") or ""), price_f, tp_f):
                continue

            if not cfg.send_enabled or dry_run:
                logging.info("[autotrade] DRY send %s %s at price=%s trigger=%s%s", code, it.get("side"), price_f, it.get("trigger_rule"), tp_f)
                continue

            if not _claim_pending(store, int(it["id"])):
                continue

            payload_raw = it.get("payload_json") or ""
            url = str(it.get("webhook_url") or "").strip()
            if not url:
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text="webhook_url_missing")
                continue
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text="payload_json_invalid")
                continue
            if not isinstance(payload, dict):
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text="payload_json_not_object")
                continue
            try:
                ok, http_status, body = _send_webhook(url, payload)
                if ok:
                    _mark_result(store, order_id=int(it["id"]), status="SENT", http_status=http_status, response_text=body)
                    maybe_notify(settings, f"[autotrade] SENT {code} {it.get('side')} trigger={it.get('trigger_rule')}{tp_f} price={price_f} http={http_status}")
                else:
                    _mark_result(store, order_id=int(it["id"]), status="ERROR", http_status=http_status, response_text=body, error_text=f"http_{http_status}")
            except Exception as exc:
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text=str(exc))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="autotrade_worker", description="Webhook auto-trade worker (plan -> queue -> dispatch).")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--dry-run", action="store_true", help="Do not send webhook even if send_enabled=true")
    return p


def main() -> None:
    ensure_repo_root()
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("logs/autotrade_worker.log"), logging.StreamHandler()],
    )

    args = build_parser().parse_args()
    settings = load_settings()
    cfg = load_autotrade_config(settings)
    if not cfg.enabled:
        raise SystemExit("autotrade disabled (config.settings.yaml autotrade.enabled=false)")

    store = SQLiteStore(cfg.db_path)
    try:
        while True:
            try:
                run_cycle(store, settings, cfg, dry_run=bool(args.dry_run))
            except Exception:
                logging.exception("[autotrade] cycle failed")
            if args.once:
                break
            time.sleep(float(cfg.poll_interval_sec))
    finally:
        try:
            store.conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
