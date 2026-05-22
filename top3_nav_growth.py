"""
top3_nav_growth.py
==================
Reads the AMFI NAV parquet output and finds the
TOP 3 mutual funds by NAV growth over the fetched period.

Run AFTER amfi_fetch.py has completed.

Usage:
    python top3_nav_growth.py
"""

import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
DATA_PATH = Path("data/amfi_nav_1y.parquet")

if not DATA_PATH.exists():
    raise FileNotFoundError(
        "data/amfi_nav_1y.parquet not found. "
        "Run amfi_fetch.py first."
    )

print("Loading data...")
df = pd.read_parquet(DATA_PATH, engine="pyarrow")
print(f"Loaded {len(df):,} rows\n")

# ---------------------------------------------------------------------------
# Strip whitespace from scheme_name so blanks don't sneak through
# ---------------------------------------------------------------------------
df["scheme_name"] = df["scheme_name"].str.strip()
df["scheme_name"] = df["scheme_name"].replace("", pd.NA)

df = df.sort_values(["scheme_code", "date"])

# ---------------------------------------------------------------------------
# Get the first NON-EMPTY scheme name per scheme_code
# (fixes missing names caused by groupby picking a blank row)
# ---------------------------------------------------------------------------
scheme_names = (
    df[df["scheme_name"].notna()]
    .groupby("scheme_code")["scheme_name"]
    .first()
    .reset_index()
)

# ---------------------------------------------------------------------------
# First and last NAV per scheme
# ---------------------------------------------------------------------------
first_nav = (
    df.groupby("scheme_code")
    .first()
    .reset_index()[["scheme_code", "nav", "date"]]
    .rename(columns={"nav": "nav_start", "date": "date_start"})
)

last_nav = (
    df.groupby("scheme_code")
    .last()
    .reset_index()[["scheme_code", "nav", "date"]]
    .rename(columns={"nav": "nav_end", "date": "date_end"})
)

# ---------------------------------------------------------------------------
# Merge names + first/last NAV
# ---------------------------------------------------------------------------
merged = first_nav.merge(last_nav, on="scheme_code")
merged = merged.merge(scheme_names, on="scheme_code", how="left")

# ---------------------------------------------------------------------------
# Calculate growth
# ---------------------------------------------------------------------------
merged["nav_growth_abs"] = (merged["nav_end"] - merged["nav_start"]).round(4)
merged["nav_growth_pct"] = (
    (merged["nav_end"] - merged["nav_start"]) / merged["nav_start"] * 100
).round(2)

# Drop funds with zero/invalid start NAV
merged = merged[merged["nav_start"] > 0].dropna(subset=["nav_growth_pct"])

# ---------------------------------------------------------------------------
# Top 3 by percentage growth
# ---------------------------------------------------------------------------
top3 = merged.nlargest(3, "nav_growth_pct").reset_index(drop=True)
top3.index += 1  # rank starts at 1

print("=" * 65)
print("  TOP 3 MUTUAL FUNDS BY NAV GROWTH (Last 1 Year)")
print("=" * 65)

for rank, row in top3.iterrows():
    name = row["scheme_name"] if pd.notna(row["scheme_name"]) else "Name unavailable"
    print(f"\n  #{rank}  {name}")
    print(f"       Scheme Code : {row['scheme_code']}")
    print(f"       NAV Start   : ₹{row['nav_start']:.4f}  ({row['date_start'].date()})")
    print(f"       NAV End     : ₹{row['nav_end']:.4f}  ({row['date_end'].date()})")
    print(f"       Growth      : ₹{row['nav_growth_abs']:.4f}  ({row['nav_growth_pct']}%)")

print("\n" + "=" * 65)

# ---------------------------------------------------------------------------
# Save full ranked list — clean column order
# ---------------------------------------------------------------------------
out = (
    merged
    .nlargest(len(merged), "nav_growth_pct")
    .reset_index(drop=True)
)[["scheme_code", "scheme_name", "nav_start", "date_start",
   "nav_end", "date_end", "nav_growth_abs", "nav_growth_pct"]]

out.index += 1
out.to_csv("data/nav_growth_ranked.csv")
print("Full ranked list saved → data/nav_growth_ranked.csv")
