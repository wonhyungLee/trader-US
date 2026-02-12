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


def _load_sector_seed(store: SQLiteStore, path: str) -> int:
    seed_path = Path(path)
    if not seed_path.exists():
        return 0
    try:
        df = pd.read_csv(seed_path)
    except Exception:
        return 0
    if df.empty:
        return 0
    cols = {str(c).strip().lower(): c for c in df.columns}
    code_col = cols.get("code") or cols.get("symbol") or cols.get("ticker") or df.columns[0]
    sector_col = cols.get("sector_name") or cols.get("sector")
    industry_col = cols.get("industry_name") or cols.get("industry")
    source_col = cols.get("source")
    rows = []
    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip().upper()
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "sector_code": None,
                "sector_name": str(r.get(sector_col, "")).strip() if sector_col else None,
                "industry_code": None,
                "industry_name": str(r.get(industry_col, "")).strip() if industry_col else None,
                "source": str(r.get(source_col, "")).strip() if source_col else "WIKI_SEED",
            }
        )
    if not rows:
        return 0
    store.upsert_sector_map(rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-partial", action="store_true", help="600개 미만이어도 강제로 적재(디버그용)")
    parser.add_argument("--no-sector-seed", action="store_true", help="sector_map_seed.csv 로드 생략")
    args = parser.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))

    df_nasdaq = load_universe_csv("data/universe_nasdaq100.csv", "NASDAQ100", "NASDAQ", "NAS")
    df_sp = load_universe_csv("data/universe_sp500.csv", "SP500", "SP500", "NYS")
    # 기대값 확인 (각 리스트 개수)
    if not args.allow_partial:
        if len(df_nasdaq) != 100 or len(df_sp) != 500:
            raise RuntimeError(
                f"universe count mismatch: nasdaq100={len(df_nasdaq)} sp500={len(df_sp)}. "
                "run scripts/generate_universe_us.py first"
            )

    df = pd.concat([df_nasdaq, df_sp], ignore_index=True)
    # 중복 티커는 그룹명을 합산 (NASDAQ100,SP500)
    df = (
        df.groupby("code", as_index=False)
        .agg(
            {
                "name": "first",
                "market": "first",
                "excd": "first",
                "group_name": lambda x: ",".join(sorted({g for g in x if g})),
            }
        )
        .sort_values("code")
    )

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
    if not args.no_sector_seed:
        loaded = _load_sector_seed(store, "data/sector_map_seed.csv")
        if loaded:
            print(f"✅ sector_map seed loaded: {loaded} rows")

    # export
    maybe_export_db(settings, store.db_path)
    print(f"✅ universe loaded: {len(df)} codes")


if __name__ == "__main__":
    main()
