# Sector-Based Universe (NASDAQ100 + S&P500)

This project uses the union of:
- `data/universe_nasdaq100.csv` (100)
- `data/universe_sp500.csv` (500)

Note: NASDAQ100 ⊂ S&P500 overlap exists, so the **unique** universe size is ~513.

Sector/industry classification is loaded from the Wikipedia seed CSV:
- `data/sector_map_seed.csv`

The sector map is stored in DB (`sector_map`) and exported to CSVs under `data/universe_sectors`.

## 1) Sector Map Build (Seed)

```bash
python -m src.collectors.sector_seed_loader --seed data/sector_map_seed.csv
```

What it does:
- Loads the seed CSV and upserts `sector_map`.
- Generates sector CSVs:
  - `data/universe_sectors/NASDAQ100/{sector_name}.csv`
  - `data/universe_sectors/SP500/{sector_name}.csv`
  - `data/universe_sectors/UNKNOWN.csv` (missing sector)

## 2) Universe CSV Update (Diff Tracking)

```bash
python -m src.collectors.universe_loader
```

Notes:
- `universe_loader` also loads `sector_map_seed.csv` by default.
- Use `--no-sector-seed` if you want to skip the seed load.

## 3) Data Freshness / Cache

Sector classification comes from the seed CSV and is reloaded when:
- You re-run `universe_loader`, or
- You run `sector_seed_loader` manually.

## 4) Acceptance Criteria Checks

- `universe_members` is the **unique** union (typically ~513)
- `sector_map.sector_name != NULL` should be **>= 95%**
- `data/universe_sectors` contains sector CSVs whose total code count is 600 (no duplicates / no missing)

Unknowns are exported to:
- `data/universe_sectors/UNKNOWN.csv`
