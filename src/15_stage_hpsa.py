"""
15_stage_hpsa.py

Stages HRSA Primary Care HPSA data to a county-level table.

Input
- data_raw/hpsa/BCD_HPSA_FCT_DET_PC.csv

Outputs
- data_staged/county_hpsa_primarycare.csv
- DuckDB table: county_hpsa_primarycare

Rules
- Discipline: Primary Care
- Designation Type: Geographic
- Status: Designated
- State FIPS 01–56 (includes DC=11)
- County score = population-weighted average HPSA score
  (fallback to simple mean if weight totals are 0)
"""

from __future__ import annotations

import os
import sys
import numpy as np
import pandas as pd
import duckdb


# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data_raw", "hpsa")
STAGED_DIR = os.path.join(BASE_DIR, "data_staged")
DB_PATH = os.path.join(BASE_DIR, "db", "ma_stars.duckdb")

RAW_CSV_NAME = "BCD_HPSA_FCT_DET_PC.csv"
RAW_CSV_PATH = os.path.join(RAW_DIR, RAW_CSV_NAME)

STAGED_CSV_PATH = os.path.join(STAGED_DIR, "county_hpsa_primarycare.csv")


# --- HRSA column names ---
COL_DISCIPLINE = "HPSA Discipline Class"
COL_DESIG_TYPE = "Designation Type"
COL_STATUS = "HPSA Status"
COL_SCORE = "HPSA Score"
COL_STATE_FIPS = "State FIPS Code"
COL_COUNTY_FIPS = "State and County Federal Information Processing Standard Code"
COL_POP = "HPSA Designation Population"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def coerce_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def coerce_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def main() -> None:
    print(f"Reading HRSA HPSA CSV: {RAW_CSV_PATH}")
    if not os.path.exists(RAW_CSV_PATH):
        die(f"Raw file not found at: {RAW_CSV_PATH}")

    os.makedirs(STAGED_DIR, exist_ok=True)

    # Read as strings to control dtype conversion
    df = pd.read_csv(RAW_CSV_PATH, dtype=str, low_memory=False)
    print(f"Loaded rows: {len(df):,}")
    print(f"Columns: {len(df.columns):,}")

    # Validate required columns
    required = [
        COL_DISCIPLINE, COL_DESIG_TYPE, COL_STATUS,
        COL_SCORE, COL_STATE_FIPS, COL_COUNTY_FIPS, COL_POP
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        die(f"Missing required columns in HRSA file: {missing}")

    # Coerce numeric columns
    df["state_fips"] = coerce_int(df[COL_STATE_FIPS])
    df["county_fips_raw"] = coerce_int(df[COL_COUNTY_FIPS])
    df["hpsa_score"] = coerce_float(df[COL_SCORE])
    df["designation_pop"] = coerce_float(df[COL_POP]).fillna(0.0)

    # Apply filters
    mask = (
        (df[COL_DISCIPLINE].fillna("") == "Primary Care") &
        (df[COL_STATUS].fillna("") == "Designated") &
        (df[COL_DESIG_TYPE].fillna("").str.contains("Geographic", case=False, na=False))
    )

    df_f = df.loc[mask].copy()
    print(f"After discipline/status/type filters: {len(df_f):,} rows")

    # Restrict to US states/DC (FIPS 01–56)
    df_f = df_f[df_f["state_fips"].between(1, 56, inclusive="both")].copy()
    print(f"After US state FIPS filter (01–56): {len(df_f):,} rows")

    # Drop rows missing county FIPS or score
    df_f = df_f.dropna(subset=["county_fips_raw", "hpsa_score"]).copy()
    print(f"After dropping missing county_fips/score: {len(df_f):,} rows")

    # Standardize county_fips to 5-character string
    df_f["county_fips"] = (
        df_f["county_fips_raw"]
        .astype(int)
        .astype(str)
        .str.zfill(5)
    )

    # Aggregate to county level (population-weighted; mean fallback)
    def agg_county(g: pd.DataFrame) -> pd.Series:
        w = g["designation_pop"].clip(lower=0.0).to_numpy()
        s = g["hpsa_score"].to_numpy()

        w_sum = float(np.nansum(w))
        if w_sum > 0:
            score = float(np.nansum(w * s) / w_sum)
            method = "pop_weighted"
        else:
            score = float(np.nanmean(s))
            method = "simple_mean"

        return pd.Series({
            "hpsa_pc_score": score,
            "hpsa_pc_flag": 1,
            "source_rows": len(g),
            "weight_method": method,
        })

    county = (
        df_f
        .groupby("county_fips", as_index=False)
        .apply(agg_county)
        .reset_index(drop=True)
    )

    print(f"County rows built: {len(county):,}")
    print("HPSA score summary (county-level):")
    print(county["hpsa_pc_score"].describe())

    # Save staged CSV
    county.to_csv(STAGED_CSV_PATH, index=False)
    print(f"Saved staged CSV: {STAGED_CSV_PATH}")

    # Write to DuckDB
    if not os.path.exists(DB_PATH):
        die(f"DuckDB not found at: {DB_PATH}")

    con = duckdb.connect(DB_PATH)
    con.register("county_hpsa", county)

    con.execute("""
        CREATE OR REPLACE TABLE county_hpsa_primarycare AS
        SELECT
            county_fips,
            CAST(hpsa_pc_score AS DOUBLE) AS hpsa_pc_score,
            CAST(hpsa_pc_flag AS INTEGER) AS hpsa_pc_flag,
            CAST(source_rows AS INTEGER) AS source_rows,
            weight_method
        FROM county_hpsa
    """)

    print("Saved DuckDB table: county_hpsa_primarycare")

    preview = con.execute(
        "SELECT * FROM county_hpsa_primarycare LIMIT 5"
    ).df()

    print("\nPreview (first 5):")
    print(preview.to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()