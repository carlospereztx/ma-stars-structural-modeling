"""
16_build_hpsa_exposure.py

Builds contract-year HPSA exposure using contract-county enrollment weights.

Inputs (DuckDB)
- contract_county_weights: contract_id, year, county_fips, w_enroll
- county_hpsa_primarycare: county_fips, hpsa_pc_score (designated counties only)

Output (DuckDB)
- contract_year_hpsa_exposure:
    contract_id
    contract_year
    hpsa_exposure                   (sum(w_enroll * hpsa_score), non-designated counties = 0)
    hpsa_designated_weight_share    (share of enrollment weight in designated counties)
    counties_total
    counties_designated
"""

from __future__ import annotations

import os
import sys

import duckdb


# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "ma_stars.duckdb")


# --- DuckDB tables ---
T_WEIGHTS = "contract_county_weights"
T_HPSA = "county_hpsa_primarycare"
T_OUT = "contract_year_hpsa_exposure"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def main() -> None:
    print(f"Connecting to DuckDB: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(DB_PATH)

    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for t in [T_WEIGHTS, T_HPSA]:
        if t not in tables:
            die(f"Required table missing: {t}")

    print(f"Building {T_OUT} (non-designated counties treated as score=0) ...")

    con.execute(f"""
        CREATE OR REPLACE TABLE {T_OUT} AS
        WITH w AS (
            SELECT
                contract_id,
                CAST(year AS INTEGER) AS contract_year,
                CAST(county_fips AS VARCHAR) AS county_fips,
                CAST(w_enroll AS DOUBLE) AS w
            FROM {T_WEIGHTS}
            WHERE w_enroll IS NOT NULL AND w_enroll > 0
        ),
        h AS (
            SELECT
                CAST(county_fips AS VARCHAR) AS county_fips,
                CAST(hpsa_pc_score AS DOUBLE) AS hpsa_pc_score
            FROM {T_HPSA}
            WHERE hpsa_pc_score IS NOT NULL
        ),
        j AS (
            SELECT
                w.contract_id,
                w.contract_year,
                w.county_fips,
                w.w,
                COALESCE(h.hpsa_pc_score, 0.0) AS hpsa_score,
                CASE WHEN h.hpsa_pc_score IS NULL THEN 0 ELSE 1 END AS is_designated
            FROM w
            LEFT JOIN h
                ON w.county_fips = h.county_fips
        )
        SELECT
            contract_id,
            contract_year,
            SUM(w * hpsa_score) AS hpsa_exposure,
            SUM(CASE WHEN is_designated = 1 THEN w ELSE 0 END) AS hpsa_designated_weight_share,
            COUNT(*) AS counties_total,
            SUM(CASE WHEN is_designated = 1 THEN 1 ELSE 0 END) AS counties_designated
        FROM j
        GROUP BY contract_id, contract_year
    """)

    n = con.execute(f"SELECT COUNT(*) FROM {T_OUT}").fetchone()[0]
    print(f"Saved DuckDB table: {T_OUT} ({n:,} rows)")

    stats = con.execute(f"""
        SELECT
            AVG(hpsa_designated_weight_share) AS avg_designated_weight_share,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY hpsa_designated_weight_share) AS p50_designated_weight_share,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY hpsa_designated_weight_share) AS p90_designated_weight_share,
            MAX(hpsa_designated_weight_share) AS max_designated_weight_share
        FROM {T_OUT}
    """).df()

    print("\nDesignated weight share summary:")
    print(stats.to_string(index=False))

    preview = con.execute(f"""
        SELECT *
        FROM {T_OUT}
        ORDER BY hpsa_designated_weight_share DESC
        LIMIT 10
    """).df()

    print("\nTop 10 contracts by designated weight share:")
    print(preview.to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()