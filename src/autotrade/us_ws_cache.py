from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Dict, Optional, Set, Tuple

import websockets

from src.brokers.kis_broker import KISBroker
from src.monitor.state_store import StateStore
from src.storage.sqlite_store import SQLiteStore, normalize_code
from src.utils.config import load_settings
from src.utils.project_root import ensure_repo_root


def _normalize_us_excd(excd: str, *, prefix: str) -> str:
    ex = str(excd or "").strip().upper()
    if ex in {"NASDAQ"}:
        ex = "NAS"
    elif ex in {"NYSE"}:
        ex = "NYS"
    elif ex in {"AMEX"}:
        ex = "AMS"

    if prefix == "R":
        if ex == "NAS":
            return "BAQ"
        if ex == "NYS":
            return "BAY"
        if ex == "AMS":
            return "BAA"
    return ex or "NAS"


def _build_tr_key(*, symbol: str, excd: str, prefix: str) -> str:
    # KIS overseas websocket key format:
    # D + EXCD(3) + SYMBOL   (free/delayed)
    # R + EXCD(3) + SYMBOL   (paid/day session, depends on market)
    ex = _normalize_us_excd(excd, prefix=prefix)
    sym = KISBroker.normalize_overseas_symbol(symbol)
    return f"{prefix}{ex}{sym}"


def _load_us_targets(store: SQLiteStore, limit: int) -> Dict[str, Tuple[str, str]]:
    """Return {code: (excd, name)} for enabled SELECTED US watchlist."""
    sql = """
    SELECT w.code,
           COALESCE(NULLIF(w.excd, ''), u.excd, '') AS excd,
           COALESCE(NULLIF(w.name, ''), u.name, '') AS name
      FROM autotrade_watchlist w
 LEFT JOIN universe_members u ON u.code = w.code
     WHERE w.list_type='SELECTED'
       AND w.enabled=1
     ORDER BY w.code
    """
    out: Dict[str, Tuple[str, str]] = {}
    for row in store.conn.execute(sql).fetchall():
        code = normalize_code(row[0])
        if not code or code.isdigit():
            continue
        excd = str(row[1] or "").strip().upper()
        name = str(row[2] or "").strip()
        out[code] = (excd, name)
        if limit > 0 and len(out) >= limit:
            break
    return out


class USWSPriceCache:
    def __init__(self, settings: dict):
        self.settings = settings
        self.autotrade_cfg = settings.get("autotrade", {}) or {}
        self.ws_cfg = (self.autotrade_cfg.get("us_ws_cache") or {})
        self.tr_id = str(self.ws_cfg.get("tr_id", "HDFSCNT0")).strip() or "HDFSCNT0"
        self.prefix = str(self.ws_cfg.get("tr_key_prefix", "D")).strip().upper() or "D"
        if self.prefix not in {"D", "R"}:
            self.prefix = "D"
        self.refresh_targets_sec = int(self.ws_cfg.get("refresh_targets_sec", 300) or 300)
        self.max_targets = int(self.ws_cfg.get("max_targets", 20) or 20)
        self.save_interval_sec = float(self.ws_cfg.get("save_interval_sec", 1.0) or 1.0)
        self.custtype = str((settings.get("kis", {}) or {}).get("custtype", "P"))

        db_path = str((settings.get("database", {}) or {}).get("path", "data/market_data.db"))
        monitor_state_path = str((settings.get("monitor", {}) or {}).get("state_path", "data/monitor_state.json"))

        self.store = SQLiteStore(db_path)
        self.state = StateStore(monitor_state_path)
        self.broker = KISBroker(settings)
        self.url = self.broker.ws_url

        self._approval_key: str = ""
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._send_lock = asyncio.Lock()

        self._targets: Dict[str, Tuple[str, str]] = {}
        self._current_keys: Set[str] = set()
        self._last_save_ts = 0.0

    async def _send(self, payload: dict) -> None:
        if not self._ws:
            return
        message = json.dumps(payload, ensure_ascii=False)
        async with self._send_lock:
            await self._ws.send(message)

    async def _subscribe_key(self, tr_key: str) -> None:
        payload = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": self.custtype,
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": self.tr_id, "tr_key": tr_key}},
        }
        await self._send(payload)

    async def _unsubscribe_key(self, tr_key: str) -> None:
        payload = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": self.custtype,
                "tr_type": "2",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": self.tr_id, "tr_key": tr_key}},
        }
        await self._send(payload)

    def _build_target_keys(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for code, (excd, _name) in self._targets.items():
            key = _build_tr_key(symbol=code, excd=excd, prefix=self.prefix)
            out[code] = key
        return out

    async def _refresh_targets(self) -> None:
        loaded = _load_us_targets(self.store, self.max_targets)
        self._targets = loaded
        key_by_code = self._build_target_keys()
        target_keys = set(key_by_code.values())

        if not self._ws:
            self._current_keys = target_keys
            self.state.current_subs = set(self._targets.keys())
            self.state.save()
            return

        to_unsub = sorted(self._current_keys - target_keys)
        to_sub = sorted(target_keys - self._current_keys)

        for key in to_unsub:
            try:
                await self._unsubscribe_key(key)
            except Exception as exc:
                logging.warning("[us-ws-cache] unsubscribe failed %s: %s", key, exc)
            await asyncio.sleep(0.2)

        for key in to_sub:
            try:
                await self._subscribe_key(key)
            except Exception as exc:
                logging.warning("[us-ws-cache] subscribe failed %s: %s", key, exc)
            await asyncio.sleep(0.2)

        self._current_keys = target_keys
        self.state.current_subs = set(self._targets.keys())
        self.state.save()
        logging.info("[us-ws-cache] targets=%s subscribed=%s", len(self._targets), len(self._current_keys))

    def _save_state_if_due(self) -> None:
        now = time.time()
        if (now - self._last_save_ts) < self.save_interval_sec:
            return
        self.state.save()
        self._last_save_ts = now

    def _handle_trade_payload(self, data_cnt: int, payload: str) -> None:
        fields = payload.split("^")
        if len(fields) < 12:
            return

        if data_cnt > 0 and len(fields) % data_cnt == 0:
            field_count = len(fields) // data_cnt
        else:
            field_count = 26

        # HDFSCNT0 expected indexes:
        # 0:RSYM, 1:SYMB, 11:LAST, 6:KYMD, 7:KHMS
        for i in range(max(1, data_cnt)):
            off = i * field_count
            if off + 11 >= len(fields):
                break
            sym = (fields[off + 1] if off + 1 < len(fields) else "").strip().upper().replace("/", ".")
            if not sym:
                continue
            try:
                price = float(str(fields[off + 11]).replace(",", "").strip())
            except Exception:
                continue
            if price <= 0:
                continue
            self.state.update_price(sym, price)

        self._save_state_if_due()

    async def _recv_loop(self) -> None:
        assert self._ws is not None
        async for message in self._ws:
            if not message:
                continue
            if message[0] in ("0", "1"):
                parts = message.split("|", 3)
                if len(parts) < 4:
                    continue
                tr_id = parts[1]
                if tr_id != self.tr_id:
                    continue
                try:
                    data_cnt = int(parts[2])
                except Exception:
                    data_cnt = 1
                self._handle_trade_payload(data_cnt, parts[3])
                continue

            try:
                obj = json.loads(message)
            except Exception:
                continue
            tr_id = (obj.get("header") or {}).get("tr_id")
            if tr_id == "PINGPONG":
                try:
                    assert self._ws is not None
                    await self._ws.send(message)
                except Exception:
                    pass

    async def _target_loop(self) -> None:
        while True:
            try:
                await self._refresh_targets()
            except Exception as exc:
                logging.warning("[us-ws-cache] target refresh failed: %s", exc)
            await asyncio.sleep(max(30, self.refresh_targets_sec))

    async def run_forever(self) -> None:
        backoff = 1
        while True:
            try:
                await self._refresh_targets()
                self._approval_key = self.broker.issue_ws_approval()
                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._ws = ws
                    self._current_keys = set()
                    logging.info("[us-ws-cache] ws connected: %s tr_id=%s prefix=%s", self.url, self.tr_id, self.prefix)
                    await self._refresh_targets()

                    recv_task = asyncio.create_task(self._recv_loop())
                    target_task = asyncio.create_task(self._target_loop())
                    done, pending = await asyncio.wait(
                        [recv_task, target_task],
                        return_when=asyncio.FIRST_EXCEPTION,
                    )
                    for task in done:
                        exc = task.exception()
                        if exc:
                            raise exc
                    for task in pending:
                        task.cancel()
                backoff = 1
            except Exception as exc:
                logging.warning("[us-ws-cache] reconnect in %ss: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            finally:
                self._ws = None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="us_ws_cache", description="US KIS websocket price cache for autotrade")
    return p


def main() -> None:
    ensure_repo_root()
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("logs/us_ws_cache.log"), logging.StreamHandler()],
    )

    _ = build_parser().parse_args()
    settings = load_settings()
    cache = USWSPriceCache(settings)
    asyncio.run(cache.run_forever())


if __name__ == "__main__":
    main()
