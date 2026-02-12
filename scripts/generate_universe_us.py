"""
NASDAQ100 / S&P500 유니버스 CSV 생성 스크립트.

- 기본은 Wikipedia 테이블 파싱(pandas.read_html) 방식입니다.
- 실행 환경에서 인터넷 연결이 필요합니다.
- 결과:
  - data/universe_nasdaq100.csv
  - data/universe_sp500.csv
  - (선택) data/sector_map_seed.csv  : 섹터/산업 정보

사용:
  python scripts/generate_universe_us.py
  python scripts/generate_universe_us.py --out-dir data
"""

from __future__ import annotations

import argparse
from pathlib import Path
import io
import pandas as pd
import requests


NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def _exchange_to_excd(value: str | None, fallback: str = "NYS") -> str:
    if not value:
        return fallback
    s = str(value).strip().upper()
    if "NASDAQ" in s:
        return "NAS"
    if "ARCA" in s or "AMEX" in s or "AMERICAN" in s:
        return "AMS"
    if "NYSE" in s:
        return "NYS"
    return fallback


def _pick_table(tables: list[pd.DataFrame], must_have: list[str]) -> pd.DataFrame:
    must = [m.lower() for m in must_have]
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if all(any(m in c for c in cols) for m in must):
            return t
    raise RuntimeError(f"no table found with columns: {must_have}")


def _read_tables(url: str) -> list[pd.DataFrame]:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    # pandas.read_html treats a raw string as a file path unless it's file-like.
    return pd.read_html(io.StringIO(resp.text))


def fetch_sp500() -> tuple[pd.DataFrame, pd.DataFrame]:
    tables = _read_tables(SP500_URL)
    t = _pick_table(tables, ["Symbol", "Security"])
    cols = {str(c).lower(): c for c in t.columns}

    sym = cols.get("symbol")
    sec = cols.get("security")
    exch = cols.get("exchange")
    sector = cols.get("gics sector") or cols.get("gics_sector") or cols.get("sector")
    industry = cols.get("gics sub-industry") or cols.get("gics sub industry") or cols.get("sub-industry") or cols.get("sub industry")

    df = pd.DataFrame({
        "code": t[sym].astype(str).str.strip(),
        "name": t[sec].astype(str).str.strip(),
        "market": "SP500",
        "excd": t[exch].astype(str).apply(lambda v: _exchange_to_excd(v, "NYS")) if exch else "NYS",
    }).drop_duplicates(subset=["code"])

    secmap = pd.DataFrame({
        "code": t[sym].astype(str).str.strip(),
        "sector_name": t[sector].astype(str).str.strip() if sector else None,
        "industry_name": t[industry].astype(str).str.strip() if industry else None,
        "source": "WIKI_SP500",
    }).drop_duplicates(subset=["code"])

    df = df[df["code"] != ""].head(500)
    secmap = secmap[secmap["code"] != ""].head(500)
    return df, secmap


def fetch_nasdaq100() -> tuple[pd.DataFrame, pd.DataFrame]:
    tables = _read_tables(NASDAQ100_URL)
    # Nasdaq-100 page tables vary; try common headings
    try:
        t = _pick_table(tables, ["Ticker", "Company"])
    except Exception:
        t = _pick_table(tables, ["Ticker", "Security"])

    cols = {str(c).lower(): c for c in t.columns}
    ticker = cols.get("ticker") or cols.get("ticker symbol") or cols.get("symbol")
    name = cols.get("company") or cols.get("security") or cols.get("company name")
    sector = cols.get("gics sector") or cols.get("sector")
    industry = cols.get("gics sub-industry") or cols.get("sub-industry") or cols.get("sub industry")

    df = pd.DataFrame({
        "code": t[ticker].astype(str).str.strip(),
        "name": t[name].astype(str).str.strip() if name else t[ticker].astype(str).str.strip(),
        "market": "NASDAQ",
        "excd": "NAS",
    }).drop_duplicates(subset=["code"])
    df = df[df["code"] != ""].head(100)

    secmap = pd.DataFrame({
        "code": t[ticker].astype(str).str.strip(),
        "sector_name": t[sector].astype(str).str.strip() if sector else None,
        "industry_name": t[industry].astype(str).str.strip() if industry else None,
        "source": "WIKI_NASDAQ100",
    }).drop_duplicates(subset=["code"])
    secmap = secmap[secmap["code"] != ""].head(100)

    return df, secmap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data", help="CSV 출력 폴더")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    nasdaq100, sec_nas = fetch_nasdaq100()
    sp500, sec_sp = fetch_sp500()

    (out_dir / "universe_nasdaq100.csv").write_text(nasdaq100.to_csv(index=False), encoding="utf-8")
    (out_dir / "universe_sp500.csv").write_text(sp500.to_csv(index=False), encoding="utf-8")

    secmap = pd.concat([sec_nas, sec_sp], ignore_index=True)
    secmap = secmap.drop_duplicates(subset=["code"])
    (out_dir / "sector_map_seed.csv").write_text(secmap.to_csv(index=False), encoding="utf-8")

    print("✅ generated:")
    print(f" - {(out_dir / 'universe_nasdaq100.csv').as_posix()} ({len(nasdaq100)} rows)")
    print(f" - {(out_dir / 'universe_sp500.csv').as_posix()} ({len(sp500)} rows)")
    print(f" - {(out_dir / 'sector_map_seed.csv').as_posix()} ({len(secmap)} rows)")


if __name__ == "__main__":
    main()
