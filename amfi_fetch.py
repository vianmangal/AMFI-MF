"""
amfi_fetch.py
=============
Production-grade ETL script to download DAILY NAV history for ALL Indian
mutual funds from the AMFI India portal for the last N years (default: 1).

Usage:
    python amfi_fetch.py --years 1 --workers 8
    python amfi_fetch.py --years 1 --workers 4 --direct-only --sqlite
    python amfi_fetch.py --years 1 --growth-only --sleep 1.5

Author : Generated for AMFI NAV ETL pipeline
Python : 3.10+
"""

# ---------------------------------------------------------------------------
# Standard Library Imports
# ---------------------------------------------------------------------------
import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ---------------------------------------------------------------------------
# Third-Party Imports
# ---------------------------------------------------------------------------
try:
    import pandas as pd
    import requests
    from requests.adapters import HTTPAdapter
    from tqdm import tqdm
    from urllib3.util.retry import Retry
except ImportError as exc:
    sys.exit(
        f"[ERROR] Missing dependency: {exc}. "
        "Run: pip install -r requirements.txt"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AMFI_URL = (
    "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
)
CHUNK_DAYS = 85          # AMFI caps ~90 days; use 85 for safety
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5     # seconds between retries (exponential)
REQUEST_TIMEOUT = 60     # seconds
DEFAULT_SLEEP = 1.0      # seconds between requests (rate limiting)
DEFAULT_WORKERS = 6
DEFAULT_YEARS = 1

# Expected RAW column order from AMFI response
RAW_COLS = [
    "scheme_code",
    "isin_growth",
    "isin_reinvestment",
    "scheme_name",
    "nav",
    "repurchase_price",
    "sale_price",
    "date",
]

FINAL_COLS = [
    "scheme_code",
    "scheme_name",
    "isin_growth",
    "isin_reinvestment",
    "nav",
    "repurchase_price",
    "sale_price",
    "date",
]

# ---------------------------------------------------------------------------
# Directory Setup
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
LOG_DIR = BASE_DIR / "logs"

for _d in (DATA_DIR, CHECKPOINT_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging Setup (console + rotating file)
# ---------------------------------------------------------------------------

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure root logger with console and rotating file handlers."""
    logger = logging.getLogger("amfi_fetch")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "[%(levelname)s] %(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler (10 MB x 5 backups)
    log_file = LOG_DIR / "amfi_fetch.log"
    fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


LOGGER = setup_logging()

# ---------------------------------------------------------------------------
# HTTP Session Factory (with retry + connection pool)
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    """
    Create a requests.Session with:
    - Connection pooling
    - Automatic retries on transient HTTP errors (500, 502, 503, 504)
    - A sensible User-Agent header
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=20,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; AMFI-NAV-Fetcher/1.0; "
                "+https://github.com/amfi-nav-fetcher)"
            ),
            "Accept": "text/plain, text/html, */*",
        }
    )
    return session


# ---------------------------------------------------------------------------
# Date Chunk Generator
# ---------------------------------------------------------------------------

def generate_date_chunks(years: int) -> list[tuple[datetime, datetime]]:
    """
    Split the last `years` into 85-day chunks.

    Returns a list of (start_date, end_date) tuples in ascending order.
    """
    end = datetime.today()
    start = end - timedelta(days=365 * years)

    chunks: list[tuple[datetime, datetime]] = []
    cursor = start

    while cursor < end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)

    LOGGER.info(
        "Generated %d date chunks covering %s → %s",
        len(chunks),
        start.strftime("%d-%b-%Y"),
        end.strftime("%d-%b-%Y"),
    )
    return chunks


# ---------------------------------------------------------------------------
# Single Chunk Fetcher
# ---------------------------------------------------------------------------

def fetch_chunk(
    session: requests.Session,
    start: datetime,
    end: datetime,
    sleep_sec: float,
) -> pd.DataFrame | None:
    """
    Download AMFI NAV data for [start, end] date range.

    Returns a cleaned DataFrame or None on unrecoverable failure.
    """
    fmt = "%d-%b-%Y"
    start_str = start.strftime(fmt)
    end_str = end.strftime(fmt)

    params = {
        "tp": 1,          # All funds
        "frmdt": start_str,
        "todt": end_str,
    }

    LOGGER.info("Fetching %s -> %s", start_str, end_str)

    try:
        resp = session.get(AMFI_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        LOGGER.error("Timeout for chunk %s -> %s", start_str, end_str)
        return None
    except requests.exceptions.RequestException as exc:
        LOGGER.error("Request failed for %s -> %s: %s", start_str, end_str, exc)
        return None
    finally:
        time.sleep(sleep_sec)  # rate limiting

    if not resp.text or len(resp.text) < 100:
        LOGGER.warning("Empty/short response for %s -> %s", start_str, end_str)
        return None

    df = parse_response(resp.text)
    if df is not None and not df.empty:
        LOGGER.info(
            "SUCCESS %s -> %s | rows: %,d",
            start_str,
            end_str,
            len(df),
        )
    return df


# ---------------------------------------------------------------------------
# Response Parser
# ---------------------------------------------------------------------------

def parse_response(raw_text: str) -> pd.DataFrame | None:
    """
    Parse AMFI pipe-delimited response text into a clean DataFrame.

    AMFI format (semicolon-separated):
        Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;
        Scheme Name;Net Asset Value;Repurchase Price;Sale Price;Date
    """
    lines = []
    for line in raw_text.splitlines():
        stripped = line.strip()
        # Skip empty lines and header rows
        if not stripped:
            continue
        if stripped.lower().startswith("scheme code"):
            continue
        parts = stripped.split(";")
        if len(parts) == 8:
            lines.append(parts)

    if not lines:
        return None

    df = pd.DataFrame(lines, columns=RAW_COLS)

    # ---- Strip whitespace from all string columns ----
    str_cols = ["scheme_code", "isin_growth", "isin_reinvestment", "scheme_name"]
    for col in str_cols:
        df[col] = df[col].str.strip()

    # ---- Parse date ----
    df["date"] = pd.to_datetime(df["date"].str.strip(), format="%d-%b-%Y", errors="coerce")

    # ---- Convert numeric columns (N.A. → NaN) ----
    for col in ("nav", "repurchase_price", "sale_price"):
        df[col] = (
            df[col]
            .str.strip()
            .replace({"N.A.": None, "NA": None, "-": None, "": None})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---- scheme_code as string (preserve leading zeros if any) ----
    df["scheme_code"] = df["scheme_code"].str.strip()

    # ---- Drop rows with missing critical fields ----
    df = df.dropna(subset=["scheme_code", "scheme_name", "nav", "date"])

    # ---- Reorder columns ----
    df = df[FINAL_COLS]

    return df


# ---------------------------------------------------------------------------
# Checkpoint Helpers
# ---------------------------------------------------------------------------

def checkpoint_path(start: datetime, end: datetime) -> Path:
    """Return the checkpoint parquet file path for a given chunk."""
    key = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    return CHECKPOINT_DIR / f"chunk_{key}.parquet"


def save_checkpoint(df: pd.DataFrame, start: datetime, end: datetime) -> None:
    """Persist a chunk DataFrame to a checkpoint parquet file."""
    path = checkpoint_path(start, end)
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    LOGGER.debug("Checkpoint saved: %s", path.name)


def load_checkpoint(start: datetime, end: datetime) -> pd.DataFrame | None:
    """Load a previously saved checkpoint if it exists."""
    path = checkpoint_path(start, end)
    if path.exists():
        LOGGER.info("Resuming from checkpoint: %s", path.name)
        return pd.read_parquet(path, engine="pyarrow")
    return None


# ---------------------------------------------------------------------------
# Concurrent Chunk Processor
# ---------------------------------------------------------------------------

def fetch_all_chunks(
    chunks: list[tuple[datetime, datetime]],
    workers: int,
    sleep_sec: float,
) -> tuple[list[pd.DataFrame], list[tuple[datetime, datetime]]]:
    """
    Fetch all date chunks concurrently using a ThreadPoolExecutor.

    Each thread gets its own HTTP session for connection-pool isolation.

    Returns:
        (successful_dataframes, failed_chunks)
    """
    results: list[pd.DataFrame] = []
    failed: list[tuple[datetime, datetime]] = []

    # Thread-local session factory
    def _worker(args: tuple[datetime, datetime]) -> tuple[pd.DataFrame | None, datetime, datetime]:
        start, end = args
        # Check checkpoint first
        cached = load_checkpoint(start, end)
        if cached is not None:
            return cached, start, end

        sess = build_session()
        df = fetch_chunk(sess, start, end, sleep_sec)
        if df is not None and not df.empty:
            save_checkpoint(df, start, end)
        return df, start, end

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_worker, chunk): chunk for chunk in chunks}

        with tqdm(total=len(chunks), desc="Downloading chunks", unit="chunk") as pbar:
            for future in as_completed(futures):
                df, start, end = future.result()
                if df is not None and not df.empty:
                    results.append(df)
                else:
                    failed.append((start, end))
                pbar.update(1)

    return results, failed


# ---------------------------------------------------------------------------
# Data Filtering
# ---------------------------------------------------------------------------

def apply_filters(
    df: pd.DataFrame,
    direct_only: bool,
    regular_only: bool,
    growth_only: bool,
) -> pd.DataFrame:
    """
    Apply optional scheme-name keyword filters.

    - direct_only  : keep only Direct schemes
    - regular_only : keep only Regular schemes
    - growth_only  : keep only Growth option schemes
    """
    name = df["scheme_name"].str.lower()

    if direct_only:
        df = df[name.str.contains("direct", na=False)]
        LOGGER.info("After --direct-only filter: %,d rows", len(df))

    if regular_only:
        df = df[~name.str.contains("direct", na=False)]
        LOGGER.info("After --regular-only filter: %,d rows", len(df))

    if growth_only:
        df = df[name.str.contains("growth", na=False)]
        LOGGER.info("After --growth-only filter: %,d rows", len(df))

    return df


# ---------------------------------------------------------------------------
# SQLite Export
# ---------------------------------------------------------------------------

def export_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    """
    Export the DataFrame to a SQLite database.

    Creates (or replaces) a table named `nav_history`.
    Adds an index on (scheme_code, date) for fast lookups.
    """
    LOGGER.info("Exporting to SQLite: %s", db_path)
    conn = sqlite3.connect(db_path)

    # Write in chunks to avoid memory pressure
    chunksize = 200_000
    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i : i + chunksize]
        if_exists = "replace" if i == 0 else "append"
        chunk.to_sql("nav_history", conn, if_exists=if_exists, index=False)
        LOGGER.debug("SQLite chunk %d written", i // chunksize + 1)

    # Create index for fast querying
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheme_date "
        "ON nav_history (scheme_code, date);"
    )
    conn.commit()
    conn.close()
    LOGGER.info("SQLite export complete: %s", db_path)


# ---------------------------------------------------------------------------
# Metadata Summary
# ---------------------------------------------------------------------------

def write_metadata(
    df: pd.DataFrame,
    failed_chunks: list[tuple[datetime, datetime]],
    successful_chunks: int,
    fetch_start_ts: float,
    out_path: Path,
) -> None:
    """Write a JSON metadata summary file."""
    elapsed = time.time() - fetch_start_ts
    unique_amcs = _extract_amc_count(df)

    meta = {
        "total_rows": int(len(df)),
        "unique_schemes": int(df["scheme_code"].nunique()),
        "unique_amcs": unique_amcs,
        "date_range": {
            "from": str(df["date"].min().date()),
            "to": str(df["date"].max().date()),
        },
        "successful_chunks": successful_chunks,
        "failed_chunks": len(failed_chunks),
        "failed_chunk_details": [
            {
                "from": s.strftime("%d-%b-%Y"),
                "to": e.strftime("%d-%b-%Y"),
            }
            for s, e in failed_chunks
        ],
        "runtime_seconds": round(elapsed, 2),
        "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
    }

    out_path.write_text(json.dumps(meta, indent=2))
    LOGGER.info("Metadata written: %s", out_path)


def _extract_amc_count(df: pd.DataFrame) -> int:
    """
    Rough AMC count: AMFI scheme names typically start with the AMC name
    followed by a space/hyphen. We extract the first word as a proxy.
    """
    try:
        amc_proxy = df["scheme_name"].str.split().str[:2].str.join(" ")
        return int(amc_proxy.nunique())
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    """End-to-end ETL orchestration."""
    pipeline_start = time.time()

    LOGGER.info("=" * 60)
    LOGGER.info("AMFI NAV Fetcher | years=%d | workers=%d | sleep=%.1fs",
                args.years, args.workers, args.sleep)
    LOGGER.info("=" * 60)

    # 1. Generate date chunks
    chunks = generate_date_chunks(args.years)

    # 2. Fetch all chunks (concurrent + checkpointed)
    dataframes, failed_chunks = fetch_all_chunks(chunks, args.workers, args.sleep)

    if not dataframes:
        LOGGER.error("No data fetched. Exiting.")
        sys.exit(1)

    # 3. Merge all chunks
    LOGGER.info("Merging %d chunk DataFrames …", len(dataframes))
    df = pd.concat(dataframes, ignore_index=True)
    LOGGER.info("Merged total rows (pre-dedup): %,d", len(df))

    # 4. Remove duplicates
    df = df.drop_duplicates(subset=["scheme_code", "date"])
    df = df.sort_values(["scheme_code", "date"]).reset_index(drop=True)
    LOGGER.info("Rows after dedup: %,d", len(df))

    # 5. Apply optional filters
    df = apply_filters(df, args.direct_only, args.regular_only, args.growth_only)

    if df.empty:
        LOGGER.error("DataFrame is empty after filters. Nothing to save.")
        sys.exit(1)

    # 6. Ensure correct dtypes one final time
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df["repurchase_price"] = pd.to_numeric(df["repurchase_price"], errors="coerce")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # 7. Save CSV
    csv_path = DATA_DIR / "amfi_nav_1y.csv"
    LOGGER.info("Saving CSV → %s", csv_path)
    df.to_csv(csv_path, index=False)
    LOGGER.info("CSV saved: %,.0f KB", csv_path.stat().st_size / 1024)

    # 8. Save Parquet (snappy compressed, columnar = fast reads)
    pq_path = DATA_DIR / "amfi_nav_1y.parquet"
    LOGGER.info("Saving Parquet → %s", pq_path)
    df.to_parquet(
        pq_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
        # Partition by date year for large datasets (optional)
        # partition_cols=["date"],
    )
    LOGGER.info("Parquet saved: %,.0f KB", pq_path.stat().st_size / 1024)

    # 9. Optional SQLite export
    if args.sqlite:
        db_path = DATA_DIR / "amfi_nav_1y.db"
        export_sqlite(df, db_path)

    # 10. Metadata JSON
    meta_path = DATA_DIR / "amfi_nav_1y_metadata.json"
    write_metadata(
        df,
        failed_chunks,
        successful_chunks=len(dataframes),
        fetch_start_ts=pipeline_start,
        out_path=meta_path,
    )

    # 11. Summary statistics
    elapsed = time.time() - pipeline_start
    LOGGER.info("=" * 60)
    LOGGER.info("SUMMARY")
    LOGGER.info("  Total rows        : %,d", len(df))
    LOGGER.info("  Unique schemes    : %,d", df["scheme_code"].nunique())
    LOGGER.info("  Date range        : %s → %s",
                df["date"].min().date(), df["date"].max().date())
    LOGGER.info("  Successful chunks : %d", len(dataframes))
    LOGGER.info("  Failed chunks     : %d", len(failed_chunks))
    LOGGER.info("  Runtime           : %.1f seconds", elapsed)
    LOGGER.info("  Output dir        : %s", DATA_DIR.resolve())
    LOGGER.info("=" * 60)

    if failed_chunks:
        LOGGER.warning(
            "%d chunk(s) failed. Re-run the script to resume from checkpoints.",
            len(failed_chunks),
        )


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download AMFI India daily NAV history for all mutual funds.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_YEARS,
        help="Number of years of history to fetch.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of concurrent download threads.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Seconds to sleep between HTTP requests (rate limiting).",
    )
    parser.add_argument(
        "--sqlite",
        action="store_true",
        default=False,
        help="Also export data to SQLite database.",
    )
    parser.add_argument(
        "--direct-only",
        dest="direct_only",
        action="store_true",
        default=False,
        help="Keep only Direct plan schemes.",
    )
    parser.add_argument(
        "--regular-only",
        dest="regular_only",
        action="store_true",
        default=False,
        help="Keep only Regular plan schemes.",
    )
    parser.add_argument(
        "--growth-only",
        dest="growth_only",
        action="store_true",
        default=False,
        help="Keep only Growth option schemes.",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level.",
    )
    return parser


if __name__ == "__main__":
    _parser = build_parser()
    _args = _parser.parse_args()

    # Reconfigure log level if changed via CLI
    logging.getLogger("amfi_fetch").setLevel(
        getattr(logging, _args.log_level.upper(), logging.INFO)
    )

    run(_args)
