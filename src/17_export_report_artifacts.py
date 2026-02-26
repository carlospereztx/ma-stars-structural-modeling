"""
17_export_report_artifacts.py

Exports report-ready CSV artifacts from DuckDB to reports/tables.

Purpose
- Make memo tables and figures reproducible without re-running the full pipeline
- Produce clean slices for near-threshold review and summary tables

Inputs (DuckDB)
- contract_year_structural_decomp_fullstars (required)
- contract_year_model_frame_fullstars (optional)

Outputs (CSV -> reports/tables/)
- decomp_full.csv
- decomp_near_threshold_30_39.csv
- decomp_near_threshold_35_39.csv
- opportunity_list_35_39.csv
- structural_buckets_summary.csv
- hpsa_deciles_effect_table.csv
- poverty_deciles_effect_table.csv
- scale_deciles_effect_table.csv
- model_inputs_snapshot.csv

Run
  python src/17_export_report_artifacts.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd


# --- Paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"
OUT_DIR = BASE_DIR / "reports" / "tables"


# --- DuckDB tables ---
T_DECOMP = "contract_year_structural_decomp_fullstars"
T_FRAME = "contract_year_model_frame_fullstars"  # optional


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def export_df(df: pd.DataFrame, filename: str) -> None:
    out_path = OUT_DIR / filename
    df.to_csv(out_path, index=False)
    print(f"Saved: {out_path} ({len(df):,} rows)")


def q(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


def main() -> None:
    print(f"Connecting to DuckDB: {DB_PATH}")
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")

    ensure_dirs()
    con = duckdb.connect(str(DB_PATH))

    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if T_DECOMP not in tables:
        die(f"Required table missing: {T_DECOMP}")

    print("\nExporting core decomposition table...")
    decomp_full = q(con, f"""
        SELECT
            contract_id,
            stars,
            expected_stars_structural,
            operational_residual,
            poverty_exposure,
            rural_exposure,
            hpsa_exposure,
            hhi,
            entropy,
            top1_share,
            top5_share,
            n_counties,
            total_enrollment,
            log_enroll,
            COALESCE(year_2025, 0) AS year_2025
        FROM {T_DECOMP}
        ORDER BY contract_id, year_2025, stars
    """)
    export_df(decomp_full, "decomp_full.csv")

    # --- Near-threshold slices ---
    print("\nExporting near-threshold slices...")
    near_30_39 = decomp_full[(decomp_full["stars"] >= 3.0) & (decomp_full["stars"] <= 3.9)].copy()
    export_df(near_30_39, "decomp_near_threshold_30_39.csv")

    near_35_39 = decomp_full[(decomp_full["stars"] >= 3.5) & (decomp_full["stars"] <= 3.9)].copy()
    export_df(near_35_39, "decomp_near_threshold_35_39.csv")

    # --- Opportunity list (3.5–3.9, most negative residuals) ---
    # Operational underperformance: residual < 0 (observed < structural expected)
    print("\nExporting opportunity list (3.5–3.9, most negative residuals)...")
    opp = near_35_39.copy()
    opp = opp.sort_values(["year_2025", "operational_residual"]).copy()

    opp_out = opp.head(50)[[
        "contract_id",
        "stars",
        "expected_stars_structural",
        "operational_residual",
        "poverty_exposure",
        "rural_exposure",
        "hpsa_exposure",
        "log_enroll",
        "total_enrollment",
        "year_2025",
    ]].copy()

    export_df(opp_out, "opportunity_list_35_39.csv")

    # --- Structural buckets summary ---
    print("\nExporting structural bucket summary...")
    buckets = decomp_full.copy()

    def classify(row) -> str:
        if row["operational_residual"] >= 0.25:
            return "Operational overperformance (above structural expectation)"
        if row["operational_residual"] <= -0.25:
            return "Operational underperformance (execution opportunity)"
        return "Near expectation (within ±0.25)"

    buckets["bucket"] = buckets.apply(classify, axis=1)

    bucket_summary = (
        buckets.groupby(["year_2025", "bucket"])
        .agg(
            n=("contract_id", "count"),
            mean_stars=("stars", "mean"),
            mean_expected=("expected_stars_structural", "mean"),
            mean_residual=("operational_residual", "mean"),
            mean_hpsa=("hpsa_exposure", "mean"),
            mean_poverty=("poverty_exposure", "mean"),
            mean_log_enroll=("log_enroll", "mean"),
        )
        .reset_index()
        .sort_values(["year_2025", "bucket"])
    )
    export_df(bucket_summary, "structural_buckets_summary.csv")

    # --- Decile effect tables ---
    print("\nExporting decile effect tables (HPSA, poverty, scale)...")
    base = decomp_full.copy()

    def decile_table(df: pd.DataFrame, col: str, label: str) -> pd.DataFrame:
        tmp = df[[col, "stars", "expected_stars_structural", "operational_residual", "year_2025"]].copy()
        tmp = tmp.dropna(subset=[col]).copy()
        tmp["decile"] = pd.qcut(tmp[col], 10, labels=False, duplicates="drop") + 1

        out = (
            tmp.groupby(["year_2025", "decile"])
            .agg(
                n=("stars", "count"),
                feature_mean=(col, "mean"),
                stars_mean=("stars", "mean"),
                expected_mean=("expected_stars_structural", "mean"),
                residual_mean=("operational_residual", "mean"),
            )
            .reset_index()
        )
        out.insert(0, "feature", label)
        return out.sort_values(["year_2025", "decile"])

    export_df(decile_table(base, "hpsa_exposure", "HPSA exposure"), "hpsa_deciles_effect_table.csv")
    export_df(decile_table(base, "poverty_exposure", "Poverty exposure"), "poverty_deciles_effect_table.csv")
    export_df(decile_table(base, "log_enroll", "Scale (log enrollment)"), "scale_deciles_effect_table.csv")

    # --- Model input snapshot (basic stats) ---
    print("\nExporting model inputs snapshot...")
    snapshot_cols = [
        "stars",
        "expected_stars_structural",
        "operational_residual",
        "poverty_exposure",
        "rural_exposure",
        "hpsa_exposure",
        "hhi",
        "entropy",
        "top1_share",
        "top5_share",
        "n_counties",
        "log_enroll",
        "total_enrollment",
        "year_2025",
    ]
    snap = decomp_full[snapshot_cols].describe().reset_index().rename(columns={"index": "stat"})
    export_df(snap, "model_inputs_snapshot.csv")

    print("\nAll exports complete.")
    print(f"Open folder: {OUT_DIR}")


if __name__ == "__main__":
    main()