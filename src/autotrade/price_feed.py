from __future__ import annotations

import logging
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import requests


_US_EXCD_ALIASES = {
    "NASDAQ": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
}
_US_DAY_EXCD = {"NAS": "BAQ", "NYS": "BAY", "AMS": "BAA"}
_US_NIGHT_EXCD = {"BAQ": "NAS", "BAY": "NYS", "BAA": "AMS"}
_US_EXCD_DEFAULT_ORDER = ("NAS", "NYS", "AMS", "BAQ", "BAY", "BAA")


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return None


def _normalize_excd(excd: Optional[str]) -> str:
    key = str(excd or "").strip().upper()
    if not key:
        return ""
    return _US_EXCD_ALIASES.get(key, key)


def _candidate_us_excds(excd: Optional[str]) -> Sequence[str]:
    ex = _normalize_excd(excd)
    out: list[str] = []
    if ex:
        out.append(ex)
    if ex in _US_DAY_EXCD:
        mapped = _US_DAY_EXCD[ex]
        if mapped not in out:
            out.append(mapped)
    if ex in _US_NIGHT_EXCD:
        mapped = _US_NIGHT_EXCD[ex]
        if mapped not in out:
            out.append(mapped)
    for item in _US_EXCD_DEFAULT_ORDER:
        if item not in out:
            out.append(item)
    return out


def _none_quote(code: str, source: str = "none") -> Dict[str, Any]:
    return {"code": code, "price": None, "asof": None, "source": source}


def _fetch_ws_cached_price(
    code: str,
    *,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    settings = settings or {}
    monitor_cfg = settings.get("monitor", {}) or {}
    autotrade_cfg = settings.get("autotrade", {}) or {}
    enabled = bool(autotrade_cfg.get("use_ws_cache_for_price", True))
    if not enabled:
        raise RuntimeError("ws_cache_disabled")

    state_path = Path(str(monitor_cfg.get("state_path", "data/monitor_state.json")))
    if not state_path.exists():
        raise RuntimeError("ws_cache_missing")

    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"ws_cache_read_failed: {exc}") from exc

    prices = payload.get("last_prices") or {}
    raw = prices.get(code)
    if raw is None:
        raise RuntimeError("ws_cache_symbol_miss")

    price = _safe_float(raw)
    if price is None or price <= 0:
        raise RuntimeError("ws_cache_price_invalid")

    max_age = int(autotrade_cfg.get("ws_cache_max_age_sec", 90) or 90)
    updated_at = _safe_float(payload.get("updated_at"))
    if updated_at is None or (time.time() - float(updated_at)) > max_age:
        raise RuntimeError("ws_cache_stale")

    return {
        "code": code,
        "price": price,
        "asof": datetime.utcfromtimestamp(float(updated_at)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "kis_ws_cache",
    }


def fetch_current_price_us(
    code: str,
    *,
    settings: Optional[Dict[str, Any]] = None,
    excd: Optional[str] = None,
    broker: Any = None,
    allow_fallback: bool = True,
) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper()
    if not symbol:
        return _none_quote("", source="none")

    try:
        return _fetch_ws_cached_price(symbol, settings=settings)
    except Exception:
        pass

    try:
        return _fetch_kis_current_price(symbol, settings=settings, excd=excd, broker=broker)
    except Exception as kis_exc:
        if not allow_fallback:
            logging.warning("[autotrade] KIS quote fetch failed for %s: %s", symbol, kis_exc)
            return _none_quote(symbol, source="kis")
        try:
            return _fetch_stooq_current_price(symbol)
        except Exception as stooq_exc:
            try:
                return _fetch_yahoo_current_price(symbol)
            except Exception as yahoo_exc:
                logging.warning(
                    "[autotrade] quote fetch failed for %s: kis=%s stooq=%s yahoo=%s",
                    symbol,
                    kis_exc,
                    stooq_exc,
                    yahoo_exc,
                )
                return _none_quote(symbol, source="none")


def _fetch_kis_current_price(
    code: str,
    *,
    settings: Optional[Dict[str, Any]] = None,
    excd: Optional[str] = None,
    broker: Any = None,
) -> Dict[str, Any]:
    symbol = str(code or "").strip().upper()
    if not symbol:
        raise ValueError("empty symbol")

    if broker is None:
        from src.brokers.kis_broker import KISBroker

        broker = KISBroker(settings)

    last_error: Optional[Exception] = None
    for market in _candidate_us_excds(excd):
        try:
            res = broker.get_overseas_current_price(market, symbol)
        except Exception as exc:
            last_error = exc
            continue

        output = res.get("output") or {}
        price = _safe_float(output.get("last") or output.get("price"))
        if price is None or price <= 0:
            last_error = RuntimeError(f"kis empty price for {symbol} ({market})")
            continue

        return {
            "code": symbol,
            "price": price,
            "asof": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "kis",
            "excd": market,
        }

    if last_error:
        raise last_error
    raise RuntimeError(f"kis quote not found for {symbol}")


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

    market_time = meta.get("regularMarketTime")
    if market_time:
        asof = datetime.utcfromtimestamp(int(market_time)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        asof = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "code": symbol,
        "price": price,
        "asof": asof,
        "source": "yahoo",
    }
