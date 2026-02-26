"""
06_stage_saipe_poverty_api.py

Fetches county-level poverty rates from the Census SAIPE API and stages them for the pipeline.

Inputs
- Census SAIPE API (county poverty, all ages)

Outputs
- data_staged/structural/county_poverty_saipe_2023.csv
- data_staged/structural/county_poverty_saipe_2024.csv
- DuckDB table: county_poverty (db/ma_stars.duckdb)

Notes
- This script pulls SAIPE years used for lagged mapping in the pipeline
  (e.g., contract-year 2024 uses 2023 poverty; contract-year 2025 uses 2024 poverty).
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests


# --- Paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"
OUT_DIR = BASE_DIR / "data_staged" / "structural"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# --- SAIPE API variables (county poverty, all ages) ---
# Rate estimate + 90% CI bounds
VARS = ["SAEPOVRTALL_PT", "SAEPOVRTALL_LB90", "SAEPOVRTALL_UB90"]


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def fetch_saipe_county_poverty(year: int) -> pd.DataFrame:
    base = "https://api.census.gov/data/timeseries/poverty/saipe"
    params = {
        "get": ",".join(["NAME"] + VARS),
        "for": "county:*",
        "in": "state:*",
        "time": str(year),
    }

    r = requests.get(base, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    df["county_fips"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)
    df["year"] = int(year)

    for c in VARS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[["year", "county_fips", "NAME"] + VARS]


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    # Years needed for lagged mapping
    years = [2023, 2024]

    frames: list[pd.DataFrame] = []
    for y in years:
        print(f"Fetching SAIPE county poverty for {y}...")
        df = fetch_saipe_county_poverty(y)

        out_path = OUT_DIR / f"county_poverty_saipe_{y}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df):,} rows -> {out_path}")

        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True)

    con.execute("DROP TABLE IF EXISTS county_poverty")
    con.register("county_poverty_df", all_df)

    con.execute("""
        CREATE TABLE county_poverty AS
        SELECT
            year,
            county_fips,
            NAME AS county_name,
            SAEPOVRTALL_PT  AS pov_rate_all,
            SAEPOVRTALL_LB90 AS pov_rate_lb90,
            SAEPOVRTALL_UB90 AS pov_rate_ub90
        FROM county_poverty_df
    """)

    print("\nRows by year:")
    print(con.execute("""
        SELECT year, COUNT(*) AS rows
        FROM county_poverty
        GROUP BY 1
        ORDER BY 1
    """).df().to_string(index=False))

    print("\nPreview:")
    print(con.execute("SELECT * FROM county_poverty LIMIT 5").df().to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()