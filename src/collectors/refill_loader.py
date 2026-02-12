from __future__ import annotations
import argparse
import json
import time
import logging
import traceback
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.collectors.kis_price_client import KISPriceClient
from src.utils.notifier import maybe_notify
from src.utils.db_exporter import maybe_export_db

# Ensure logs are visible
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OVRS_INFO_CACHE_PATH = Path("data/ovrs_stock_info_cache.json")


class AuthForbiddenError(Exception):
    pass


def _is_auth_forbidden_error(exc: Exception) -> bool:
    msg = str(exc)
    return "403" in msg and "tokenP" in msg

def read_universe(paths: Iterable[str]) -> List[str]:
    codes: List[str] = []
    for p in paths:
        if not Path(p).exists():
            continue
        df = pd.read_csv(p)
        col = "code" if "code" in df.columns else "Code" if "Code" in df.columns else df.columns[0]
        codes.extend(df[col].astype(str).str.strip().tolist())
    seen = set()
    uniq = []
    for c in codes:
        c = str(c).strip()
        if not c:
            continue
        c = c.upper()
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


def _load_info_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in data.items():
        if isinstance(v, dict):
            out[str(k).upper()] = v
    return out


def _save_info_cache(path: Path, data: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {str(k).upper(): v for k, v in data.items()}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_listing_date(value: object) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) != 8 or digits.startswith("0000"):
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return None


def _normalize_excd(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip().upper()
    if s.startswith("NAS"):
        return "NAS"
    if s.startswith("NYS") or s.startswith("NYSE"):
        return "NYS"
    if s.startswith("AMS") or s.startswith("AMEX"):
        return "AMS"
    return None


def _prdt_type_from_excd(excd: Optional[str]) -> Optional[str]:
    if not excd:
        return None
    excd = excd.upper()
    if excd == "NAS":
        return "512"
    if excd == "NYS":
        return "513"
    if excd == "AMS":
        return "529"
    return None


def _extract_ovrs_info(res: Dict[str, Any], prdt_type_cd: str) -> Optional[Dict[str, Any]]:
    output = res.get("output") or res.get("output1") or {}
    if isinstance(output, list):
        output = output[0] if output else {}
    if not isinstance(output, dict):
        return None
    listed_date = _normalize_listing_date(output.get("lstg_dt"))
    excd = _normalize_excd(output.get("ovrs_excg_cd"))
    return {
        "code": str(output.get("std_pdno") or output.get("pdno") or "").strip() or None,
        "listed_date": listed_date,
        "excd": excd,
        "exchange_name": output.get("ovrs_excg_name") or output.get("tr_mket_name"),
        "currency": output.get("tr_crcy_cd"),
        "country": output.get("natn_name"),
        "prdt_type_cd": prdt_type_cd,
    }


def get_overseas_info(
    client: KISPriceClient,
    symbol: str,
    excd_hint: Optional[str],
    cache: Dict[str, Dict[str, Any]],
    cache_path: Path,
    refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    key = symbol.upper()
    if not refresh and key in cache:
        return cache.get(key)

    prdt_candidates: List[str] = []
    hint = _prdt_type_from_excd(excd_hint)
    if hint:
        prdt_candidates.append(hint)
    for cand in ("512", "513", "529"):
        if cand not in prdt_candidates:
            prdt_candidates.append(cand)

    for prdt in prdt_candidates:
        try:
            res = client.get_overseas_stock_info(prdt, key)
        except Exception:
            continue
        if str(res.get("rt_cd")) not in ("0", "OK", "success"):
            # still try to parse output if present
            pass
        info = _extract_ovrs_info(res, prdt)
        if info and (info.get("listed_date") or info.get("excd") or info.get("exchange_name")):
            cache[key] = info
            _save_info_cache(cache_path, cache)
            return info
    return None


def _parse_overseas_daily(res: dict) -> pd.DataFrame:
    outputs = res.get("output2") or res.get("output") or []
    if not isinstance(outputs, list) or not outputs:
        return pd.DataFrame()
    recs = []
    for o in outputs:
        close = float(o.get("clos") or o.get("close") or 0)
        volume = float(o.get("tvol") or 0)
        amount = float(o.get("tamt") or 0)
        if amount <= 0 and close > 0 and volume > 0:
            amount = close * volume
        recs.append(
            {
                "date": o.get("xymd"),
                "open": float(o.get("open") or 0),
                "high": float(o.get("high") or 0),
                "low": float(o.get("low") or 0),
                "close": close,
                "volume": volume,
                "amount": amount,
            }
        )
    df = pd.DataFrame(recs)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])
    if df.empty:
        return df
    df = df.sort_values("date")
    df["ma25"] = df["close"].rolling(25, min_periods=5).mean()
    df["disparity"] = df["close"] / df["ma25"] - 1
    return df[["date", "open", "high", "low", "close", "volume", "amount", "ma25", "disparity"]]


def fetch_prices_kis_overseas(client: KISPriceClient, excd: str, symbol: str, end_date: str) -> pd.DataFrame:
    res = client.get_overseas_daily_prices(excd, symbol, end_date.replace("-", ""))
    return _parse_overseas_daily(res)


def backward_refill(
    store: SQLiteStore,
    code: str,
    excd: str,
    chunk_days: int,
    sleep: float,
    empty_limit: int = 3,
    kis_client: Optional[KISPriceClient] = None,
    notify_cb=None,
    notify_every: int = 1,
    resume_end: Optional[str] = None,
    min_date: Optional[str] = None,
    auth_cooldown: Optional[float] = None,
):
    today = datetime.today().date()
    current_end = datetime.strptime(resume_end, "%Y-%m-%d").date() if resume_end else today
    min_date_dt = datetime.strptime(min_date, "%Y-%m-%d").date() if min_date else None
    
    empty_cnt = 0
    last_min_date: Optional[str] = None
    chunk_idx = 0

    while True:
        if min_date_dt and current_end < min_date_dt:
            print(f"[{code}] Reached listing date {min_date_dt:%Y-%m-%d}. Stop.")
            break
        start_date = current_end - timedelta(days=chunk_days)
        chunk_idx += 1
        
        print(f"[{code}] Chunk {chunk_idx}: fetching up to {current_end:%Y-%m-%d}...")
        
        try:
            df = fetch_prices_kis_overseas(
                kis_client,
                excd,
                code,
                current_end.strftime("%Y-%m-%d"),
            )  # type: ignore
        except Exception as e:
            if _is_auth_forbidden_error(e):
                cooldown = float(auth_cooldown or 0)
                print(f"[{code}] 403 tokenP detected. Cooling down {cooldown:.1f}s and clearing cache.")
                if cooldown > 0:
                    time.sleep(cooldown)
                if kis_client is not None:
                    try:
                        kis_client.broker.clear_token_cache()
                        kis_client.broker.reset_sessions()
                    except Exception:
                        pass
                continue
            print(f"[{code}] API Error: {e}")
            empty_cnt += 1
            time.sleep(sleep * 5)
            if empty_cnt >= empty_limit:
                break
            continue

        reached_min_date = False
        if df.empty:
            print(f"[{code}] Empty response at {current_end:%Y-%m-%d}")
            empty_cnt += 1
        else:
            min_date_str = df["date"].min()
            chunk_min_date = datetime.strptime(min_date_str, "%Y-%m-%d").date()
            
            if last_min_date and min_date_str >= last_min_date:
                print(f"[{code}] Duplicate/No-earlier data at {min_date_str}")
                empty_cnt += 1
            else:
                empty_cnt = 0
                last_min_date = min_date_str
                store.upsert_daily_prices(code, df)
                current_end = chunk_min_date - timedelta(days=1)
                print(f"[{code}] Saved {len(df)} rows. Next end: {current_end:%Y-%m-%d}")
                if min_date_dt and chunk_min_date <= min_date_dt:
                    reached_min_date = True

        store.upsert_refill_status(
            code=code,
            next_end=current_end.strftime("%Y-%m-%d"),
            last_min=last_min_date,
            status="RUNNING",
            message=f"chunk={chunk_idx} empty={empty_cnt}",
        )

        if reached_min_date:
            print(f"[{code}] Listing date reached ({min_date_dt:%Y-%m-%d}).")
            break
        if empty_cnt >= empty_limit:
            print(f"[{code}] Stopped: empty limit reached.")
            break
        if current_end.year < 1980:
            print(f"[{code}] Stopped: year limit reached.")
            break

        time.sleep(sleep)

    done_msg = f"chunks={chunk_idx}"
    if min_date_dt:
        done_msg = f"{done_msg} listing={min_date_dt:%Y-%m-%d}"
    store.upsert_refill_status(
        code=code,
        next_end=current_end.strftime("%Y-%m-%d"),
        last_min=last_min_date,
        status="DONE",
        message=done_msg,
    )


def main():
    print("MAIN START")
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", action="append", help="CSV 파일 경로", default=[])
    parser.add_argument("--code", help="단일 종목 코드", default=None)
    parser.add_argument("--chunk-days", type=int, default=150)
    parser.add_argument("--start-mode", choices=["listing", "backward"], default="listing")
    parser.add_argument("--listing-cache", default=None, help="상품기본정보 캐시 JSON 경로")
    parser.add_argument("--refresh-listing", action="store_true", help="캐시 무시하고 재조회")
    parser.add_argument("--sleep", type=float, default=None, help="호출 간격 override (sec)")
    parser.add_argument("--resume", action="store_true", help="중단 지점부터 재개")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings()
    db_path = settings.get("database", {}).get("path", "data/market_data.db")
    store = SQLiteStore(db_path)
    job_id = store.start_job("refill_loader")
    
    kis_client = KISPriceClient(settings)
    kis_client.broker.reset_sessions()

    sleep = float(args.sleep) if args.sleep is not None else float(settings.get("kis", {}).get("rate_limit_sleep_sec", 0.5))

    info_cache_path = Path(args.listing_cache) if args.listing_cache else OVRS_INFO_CACHE_PATH
    info_cache: Dict[str, Dict[str, Any]] = {}
    if args.start_mode == "listing":
        info_cache = _load_info_cache(info_cache_path)

    if args.code:
        codes = [args.code.strip().upper()]
    elif args.universe:
        codes = read_universe(args.universe)
    else:
        codes = store.list_universe_codes()
    
    if not codes:
        print("Error: No codes to process.")
        return

    if args.limit:
        codes = codes[: args.limit]

    # Calculate global totals
    all_universe_codes = store.list_universe_codes() if not args.universe else read_universe(args.universe)
    total_universe_count = len(all_universe_codes)

    universe_df = store.load_universe_df()
    group_map = {row["code"]: row.get("group_name") for _, row in universe_df.iterrows()}
    excd_map = store.list_universe_excd_map()

    def _fallback_excd(code: str) -> str:
        group = str(group_map.get(code, "")).upper()
        return "NAS" if "NASDAQ" in group else "NYS"

    print(f"Processing {len(codes)} codes...")
    processed_in_this_run = 0
    try:
        for code in codes:
            status = store.get_refill_status(code)
            if args.resume and status and status["status"] == "DONE":
                continue
            
            resume_end = status["next_end_date"] if status and status["next_end_date"] else None
            excd = excd_map.get(code) or _fallback_excd(code)
            listing_date = None
            info = None
            if args.start_mode == "listing":
                info = get_overseas_info(
                    kis_client,
                    code,
                    excd,
                    info_cache,
                    info_cache_path,
                    refresh=args.refresh_listing,
                )
                if info:
                    excd = info.get("excd") or excd
                    listing_date = info.get("listed_date")
                    store.upsert_ovrs_stock_info([{
                        "code": code,
                        "excd": excd,
                        "prdt_type_cd": info.get("prdt_type_cd"),
                        "listed_date": listing_date,
                        "exchange_name": info.get("exchange_name"),
                        "currency": info.get("currency"),
                        "country": info.get("country"),
                    }])
                    if listing_date:
                        print(f"[{code}] Listing date: {listing_date}")
                else:
                    print(f"[{code}] Listing info not found; fallback to backward scan.")
            
            print(f"=== Starting {code} ({processed_in_this_run+1}/{len(codes)}) ===")
            try:
                backward_refill(
                    store,
                    code,
                    excd,
                    args.chunk_days,
                    sleep,
                    kis_client=kis_client,
                    resume_end=resume_end,
                    min_date=listing_date,
                    auth_cooldown=settings.get("kis", {}).get("auth_forbidden_cooldown_sec", 600),
                )
                processed_in_this_run += 1
                
                # Export DB to CSV after EACH stock
                maybe_export_db(settings, store.db_path)
                
                # Get global done count
                done_count = store.conn.execute("SELECT count(*) FROM refill_progress WHERE status='DONE'").fetchone()[0]
                
                # Notify after CSV export
                total = max(total_universe_count, 1)
                pct = (done_count / total) * 100.0
                remaining = max(total - done_count, 0)
                msg = (
                    f"✅ [refill] {code} 완료 및 CSV 저장됨 "
                    f"({done_count}/{total_universe_count}, {pct:.1f}%, remaining {remaining})"
                )
                maybe_notify(settings, msg)
                
                # Prevent Discord rate limit
                time.sleep(0.5)

            except Exception as e:
                print(f"Error processing {code}: {e}")
                traceback.print_exc()
                store.upsert_refill_status(code, resume_end, None, "ERROR", str(e))
                maybe_notify(settings, f"❌ [refill] {code} 오류: {e}")
        
        store.finish_job(job_id, "SUCCESS", f"processed={processed_in_this_run}")
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()
        store.finish_job(job_id, "ERROR", str(e))
    finally:
        maybe_export_db(settings, store.db_path)
        try:
            done_total = store.conn.execute("SELECT count(*) FROM refill_progress WHERE status='DONE'").fetchone()[0]
        except Exception:
            done_total = None
        if done_total is not None:
            msg = f"🏁 [refill] 전체 작업 종료 (이번 실행 {processed_in_this_run}개, 누적 {done_total}/{total_universe_count})"
        else:
            msg = f"🏁 [refill] 전체 작업 종료 (이번 실행 {processed_in_this_run}개)"
        maybe_notify(settings, msg)

if __name__ == "__main__":
    main()
