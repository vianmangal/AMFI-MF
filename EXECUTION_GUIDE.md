# AMFI NAV Fetcher — Execution Guide

## Prerequisites

- Python 3.10 or higher
- pip (comes with Python)
- Internet access to portal.amfiindia.com

---

## 1. Setup

```bash
# Clone or copy files into a working directory
mkdir amfi_nav && cd amfi_nav
cp amfi_fetch.py requirements.txt .

# Create a virtual environment (recommended)
python -m venv .venv

# Activate it
# Linux/macOS:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## 2. Basic Usage

```bash
# Fetch last 1 year of NAV data (default)
python amfi_fetch.py

# Specify years and worker count explicitly
python amfi_fetch.py --years 1 --workers 8
```

---

## 3. Advanced Options

```bash
# Slower, gentler fetch (1.5s sleep between requests)
python amfi_fetch.py --sleep 1.5

# Also export to SQLite
python amfi_fetch.py --sqlite

# Direct plans only
python amfi_fetch.py --direct-only

# Regular plans only
python amfi_fetch.py --regular-only

# Growth option only
python amfi_fetch.py --growth-only

# Combine filters and SQLite
python amfi_fetch.py --direct-only --growth-only --sqlite

# Debug-level logging
python amfi_fetch.py --log-level DEBUG
```

---

## 4. Output Files

All outputs are written to the `data/` directory:

| File                        | Description                          |
|-----------------------------|--------------------------------------|
| `data/amfi_nav_1y.csv`      | Full NAV history as CSV              |
| `data/amfi_nav_1y.parquet`  | Same data as compressed Parquet      |
| `data/amfi_nav_1y.db`       | SQLite DB (only with `--sqlite`)     |
| `data/amfi_nav_1y_metadata.json` | Run summary (rows, runtime, etc.) |
| `checkpoints/chunk_*.parquet` | Per-chunk files for resume support |
| `logs/amfi_fetch.log`       | Rotating log file                    |

---

## 5. Resume After Interruption

If the script is interrupted mid-run, simply re-run the same command.
It will automatically skip already-downloaded chunks (loaded from `checkpoints/`):

```bash
python amfi_fetch.py --years 1 --workers 8
# [INFO] Resuming from checkpoint: chunk_20240519_20240812.parquet
```

---

## 6. Sample Console Output

```
[INFO] 2025-05-19 10:00:01 | ==============================
[INFO] 2025-05-19 10:00:01 | AMFI NAV Fetcher | years=1 | workers=6 | sleep=1.0s
[INFO] 2025-05-19 10:00:01 | ==============================
[INFO] 2025-05-19 10:00:01 | Generated 5 date chunks covering 19-May-2024 → 19-May-2025
Downloading chunks: 100%|████████████| 5/5 [01:42<00:00, 20.5s/chunk]
[INFO] 2025-05-19 10:01:43 | Merging 5 chunk DataFrames …
[INFO] 2025-05-19 10:01:44 | Merged total rows (pre-dedup): 3,850,000
[INFO] 2025-05-19 10:01:45 | Rows after dedup: 3,847,200
[INFO] 2025-05-19 10:01:47 | Saving CSV → data/amfi_nav_1y.csv
[INFO] 2025-05-19 10:02:10 | CSV saved: 245,300 KB
[INFO] 2025-05-19 10:02:10 | Saving Parquet → data/amfi_nav_1y.parquet
[INFO] 2025-05-19 10:02:18 | Parquet saved: 38,200 KB
[INFO] 2025-05-19 10:02:18 | ==============================
[INFO] 2025-05-19 10:02:18 | SUMMARY
[INFO] 2025-05-19 10:02:18 |   Total rows        : 3,847,200
[INFO] 2025-05-19 10:02:18 |   Unique schemes    : 11,400
[INFO] 2025-05-19 10:02:18 |   Date range        : 2024-05-20 → 2025-05-19
[INFO] 2025-05-19 10:02:18 |   Successful chunks : 5
[INFO] 2025-05-19 10:02:18 |   Failed chunks     : 0
[INFO] 2025-05-19 10:02:18 |   Runtime           : 137.4 seconds
[INFO] 2025-05-19 10:02:18 |   Output dir        : /home/user/amfi_nav/data
[INFO] 2025-05-19 10:02:18 | ==============================
```

---

## 7. Sample metadata JSON

```json
{
  "total_rows": 3847200,
  "unique_schemes": 11400,
  "unique_amcs": 45,
  "date_range": {
    "from": "2024-05-20",
    "to": "2025-05-19"
  },
  "successful_chunks": 5,
  "failed_chunks": 0,
  "failed_chunk_details": [],
  "runtime_seconds": 137.4,
  "fetch_timestamp": "2025-05-19T04:32:18Z"
}
```

---

## 8. DataFrame Schema

| Column             | Type      | Notes                          |
|--------------------|-----------|--------------------------------|
| scheme_code        | str       | AMFI scheme code               |
| scheme_name        | str       | Full scheme name               |
| isin_growth        | str       | ISIN for Growth / Div Payout   |
| isin_reinvestment  | str       | ISIN for Div Reinvestment      |
| nav                | float64   | Net Asset Value                |
| repurchase_price   | float64   | Redemption price (NaN if N.A.) |
| sale_price         | float64   | Purchase price (NaN if N.A.)   |
| date               | datetime  | NAV date                       |

---

## 9. Tips

- Use `--workers 4` on slower connections or if AMFI rate-limits you.
- The default `--sleep 1.0` is polite; reduce only if you need speed.
- Parquet files are ~6–8× smaller than CSV and load much faster in pandas.
- To reload the final data: `pd.read_parquet("data/amfi_nav_1y.parquet")`
- Delete `checkpoints/` folder to force a full re-download.
