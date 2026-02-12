"""유니버스(600개) 스냅샷 로더 (US).

- 대상: NASDAQ100 + S&P500
- 입력:
  - data/universe_nasdaq100.csv
  - data/universe_sp500.csv

CSV 최소 컬럼:
  - code (또는 symbol/ticker)
  - name (optional)
  - market (optional)

주의: 최초 실행 전 scripts/generate_universe_us.py 로 CSV를 생성하는 것을 권장합니다.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings
from src.utils.db_exporter import maybe_export_db


def _normalize_excd(value: object, fallback: str) -> str:
    if value is None:
        return fallback
    s = str(value).strip().upper()
    if not s:
        return fallback
    if "NASDAQ" in s or s.startswith("NAS"):
        return "NAS"
    if "ARCA" in s or "AMEX" in s or "AMERICAN" in s:
        return "AMS"
    if "NYSE" in s or s.startswith("NYS"):
        return "NYS"
    if s in {"NAS", "NYS", "AMS"}:
        return s
    return fallback


def load_universe_csv(path: str, group_name: str, default_market: str, default_excd: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"missing universe file: {path}")
    df = pd.read_csv(p)
    if df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "excd", "group_name"])
    cols = {c.lower(): c for c in df.columns}

    code_col = cols.get("code") or cols.get("symbol") or cols.get("ticker") or df.columns[0]
    name_col = cols.get("name") or cols.get("company") or (df.columns[1] if len(df.columns) > 1 else None)
    mkt_col = cols.get("market")
    excd_col = cols.get("excd") or cols.get("exchange")

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str).str.strip()
    out["name"] = df[name_col].astype(str).str.strip() if name_col else out["code"]
    out["market"] = df[mkt_col].astype(str).str.strip() if mkt_col and mkt_col in df.columns else default_market
    if excd_col and excd_col in df.columns:
        out["excd"] = df[excd_col].apply(lambda v: _normalize_excd(v, default_excd))
    else:
        out["excd"] = default_excd
    out["group_name"] = group_name
    out = out.drop_duplicates(subset=["code"])
    out = out[out["code"] != ""]
    return out[["code", "name", "market", "excd", "group_name"]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-partial", action="store_true", help="600개 미만이어도 강제로 적재(디버그용)")
    args = parser.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))

    df_nasdaq = load_universe_csv("data/universe_nasdaq100.csv", "NASDAQ100", "NASDAQ", "NAS")
    df_sp = load_universe_csv("data/universe_sp500.csv", "SP500", "SP500", "NYS")
    df = pd.concat([df_nasdaq, df_sp], ignore_index=True)

    # 기대값 확인
    if len(df) != 600 and not args.allow_partial:
        raise RuntimeError(f"universe count must be 600, got {len(df)}. run scripts/generate_universe_us.py first")

    # Snapshot diff 기록
    cur = store.conn.execute("SELECT code, market FROM universe_members")
    old = {(r[0], r[1]) for r in cur.fetchall()}
    new = {(r["code"], r["market"]) for _, r in df.iterrows()}
    added = sorted(
        [{"code": c, "market": m} for (c, m) in (new - old)],
        key=lambda x: (x["code"], x["market"]),
    )
    removed = sorted(
        [{"code": c, "market": m} for (c, m) in (old - new)],
        key=lambda x: (x["code"], x["market"]),
    )

    snapshot_date = datetime.utcnow().strftime("%Y-%m-%d")
    store.conn.execute(
        "INSERT INTO universe_changes(snapshot_date, market, added_codes_json, removed_codes_json) VALUES (?,?,?,?)",
        (snapshot_date, "US", json.dumps(added, ensure_ascii=False), json.dumps(removed, ensure_ascii=False)),
    )

    store.upsert_universe_members(df.to_dict(orient="records"))

    # export
    maybe_export_db(settings, store.db_path)
    print(f"✅ universe loaded: {len(df)} codes")


if __name__ == "__main__":
    main()
