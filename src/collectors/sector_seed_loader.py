from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.storage.sqlite_store import SQLiteStore
from src.utils.config import load_settings


def _sanitize_filename(value: str) -> str:
    if not value:
        return "UNKNOWN"
    s = value.strip().replace("/", "-")
    s = s.replace("\\", "-")
    s = s.replace(":", "-")
    return s


def _pick_col(cols: Dict[str, str], *candidates: str) -> Optional[str]:
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


def load_sector_seed(path: Path, source: str) -> List[Dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(f"seed CSV not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        return []
    cols = {str(c).strip().lower(): c for c in df.columns}

    code_col = _pick_col(cols, "code", "symbol", "ticker") or df.columns[0]
    sector_col = _pick_col(cols, "sector_name", "sector")
    industry_col = _pick_col(cols, "industry_name", "industry")
    source_col = _pick_col(cols, "source")

    rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip().upper()
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "sector_code": None,
                "sector_name": str(row.get(sector_col, "")).strip() if sector_col else None,
                "industry_code": None,
                "industry_name": str(row.get(industry_col, "")).strip() if industry_col else None,
                "updated_at": datetime.utcnow().isoformat(),
                "source": str(row.get(source_col, "")).strip() if source_col else source,
            }
        )
    return rows


def build_sector_csvs(store: SQLiteStore, out_root: Path) -> dict:
    conn = store.conn
    df = pd.read_sql_query(
        """
        SELECT u.code, u.name, u.market, u.group_name, s.sector_code, s.sector_name, s.industry_code, s.industry_name
        FROM universe_members u
        LEFT JOIN sector_map s ON u.code = s.code
        ORDER BY u.code
        """,
        conn,
    )
    if df.empty:
        return {"total": 0, "unknown": 0, "files": 0}

    df["sector_name"] = df["sector_name"].fillna("UNKNOWN")
    df["sector_code"] = df["sector_code"].fillna("")
    df["industry_name"] = df["industry_name"].fillna("")
    df["industry_code"] = df["industry_code"].fillna("")

    out_root.mkdir(parents=True, exist_ok=True)
    total = len(df)
    unknown_df = df[df["sector_name"] == "UNKNOWN"].copy()
    unknown_path = out_root / "UNKNOWN.csv"
    unknown_df.to_csv(
        unknown_path,
        index=False,
        columns=["code", "name", "market", "group_name", "sector_name", "sector_code", "industry_name", "industry_code"],
    )

    files = 1
    for group, gdf in df.groupby("group_name", dropna=False):
        group_name = str(group or "UNKNOWN").strip() or "UNKNOWN"
        group_dir = out_root / _sanitize_filename(group_name)
        group_dir.mkdir(parents=True, exist_ok=True)
        for sector, sdf in gdf.groupby("sector_name"):
            fname = _sanitize_filename(str(sector))
            out_path = group_dir / f"{fname}.csv"
            sdf[
                ["code", "name", "market", "group_name", "sector_name", "sector_code", "industry_name", "industry_code"]
            ].to_csv(out_path, index=False)
            files += 1

    return {"total": total, "unknown": len(unknown_df), "files": files}


def main():
    ap = argparse.ArgumentParser(description="Load sector_map from seed CSV (US).")
    ap.add_argument("--seed", default="data/sector_map_seed.csv", help="seed CSV path")
    ap.add_argument("--source", default="WIKI_SEED", help="source label")
    ap.add_argument("--no-csv", action="store_true", help="skip building sector CSVs")
    args = ap.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.get("database", {}).get("path", "data/market_data.db"))
    seed_path = Path(args.seed)
    rows = load_sector_seed(seed_path, args.source)
    if not rows:
        print("No sector rows loaded.")
        return
    store.upsert_sector_map(rows)
    print(f"✅ sector_map updated: {len(rows)} rows from {seed_path}")

    if not args.no_csv:
        summary = build_sector_csvs(store, Path("data/universe_sectors"))
        print(
            "✅ sector CSVs built: total={total} unknown={unknown} files={files}".format(**summary)
        )


if __name__ == "__main__":
    main()
