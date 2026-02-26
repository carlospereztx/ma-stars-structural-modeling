"""
07_build_poverty_exposure.py

Builds lagged, enrollment-weighted SAIPE poverty exposure at the contract-year level.

Inputs (DuckDB)
- contract_county_weights (contract_id, year, county_fips, contract_year_total_enrollment, w_enroll)
- county_poverty (year, county_fips, pov_rate_all)

Outputs (DuckDB)
- contract_year_poverty_missing_share
- contract_year_missing_poverty
- contract_year_poverty_exposure_clean

Notes
- Lag rule: contract-year t uses SAIPE poverty year (t - 1)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    # --- 0) Required tables ---
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for t in ["contract_county_weights", "county_poverty"]:
        if t not in tables:
            die(f"Required table missing: {t}")

    # --- 1) Missing poverty-weight share per contract-year ---
    con.execute("""
        CREATE OR REPLACE TABLE contract_year_poverty_missing_share AS
        SELECT
            w.contract_id,
            w.year AS contract_year,
            SUM(w.w_enroll * CASE WHEN p.county_fips IS NULL THEN 1 ELSE 0 END) AS missing_share
        FROM contract_county_weights w
        LEFT JOIN county_poverty p
            ON p.county_fips = w.county_fips
           AND p.year = w.year - 1
        GROUP BY 1, 2
    """)

    print("\nMissing-weight share diagnostics (per contract-year):")
    print(con.execute("""
        SELECT
            contract_year,
            AVG(missing_share) AS avg_missing_share,
            MAX(missing_share) AS max_missing_share
        FROM contract_year_poverty_missing_share
        GROUP BY 1
        ORDER BY 1
    """).df().to_string(index=False))

    # --- 2) Contract-years entirely outside SAIPE coverage (missing_share = 1.0) ---
    con.execute("""
        CREATE OR REPLACE TABLE contract_year_missing_poverty AS
        SELECT
            contract_id,
            contract_year,
            missing_share
        FROM contract_year_poverty_missing_share
        WHERE missing_share = 1.0
        ORDER BY contract_year, contract_id
    """)

    print("\nOut-of-scope contract-years (missing_share = 1.0):")
    print(con.execute("""
        SELECT *
        FROM contract_year_missing_poverty
        ORDER BY contract_year, contract_id
        LIMIT 50
    """).df().to_string(index=False))

    # --- 3) Exposure table (renormalized to matched weight only) ---
    con.execute("""
        CREATE OR REPLACE TABLE contract_year_poverty_exposure_clean AS
        WITH joined AS (
            SELECT
                w.contract_id,
                w.year AS contract_year,
                w.contract_year_total_enrollment,
                w.w_enroll,
                p.pov_rate_all
            FROM contract_county_weights w
            LEFT JOIN county_poverty p
                ON p.county_fips = w.county_fips
               AND p.year = w.year - 1
        ),
        agg AS (
            SELECT
                contract_id,
                contract_year,
                MAX(contract_year_total_enrollment) AS total_enrollment,
                SUM(CASE WHEN pov_rate_all IS NOT NULL THEN w_enroll * pov_rate_all ELSE 0 END) AS num,
                SUM(CASE WHEN pov_rate_all IS NOT NULL THEN w_enroll ELSE 0 END) AS denom
            FROM joined
            GROUP BY 1, 2
        )
        SELECT
            contract_id,
            contract_year,
            total_enrollment,
            CASE WHEN denom > 0 THEN num / denom ELSE NULL END AS poverty_exposure,
            denom AS coverage_weight_share,
            CASE WHEN denom = 0 THEN 1 ELSE 0 END AS out_of_scope_geography
        FROM agg
    """)

    print("\nSample exposures (highest poverty_exposure):")
    print(con.execute("""
        SELECT *
        FROM contract_year_poverty_exposure_clean
        WHERE out_of_scope_geography = 0
        ORDER BY contract_year, poverty_exposure DESC
        LIMIT 10
    """).df().to_string(index=False))

    print("\nCoverage summary:")
    print(con.execute("""
        SELECT
            contract_year,
            COUNT(*) AS contract_years,
            SUM(out_of_scope_geography) AS out_of_scope_count,
            AVG(coverage_weight_share) AS avg_coverage_weight_share,
            MIN(coverage_weight_share) AS min_coverage_weight_share,
            MAX(coverage_weight_share) AS max_coverage_weight_share
        FROM contract_year_poverty_exposure_clean
        GROUP BY 1
        ORDER BY 1
    """).df().to_string(index=False))

    # --- 4) Row-level join coverage (debug) ---
    print("\nRow-level join coverage:")
    print(con.execute("""
        SELECT
            w.year AS contract_year,
            COUNT(*) AS weight_rows,
            SUM(CASE WHEN p.county_fips IS NULL THEN 1 ELSE 0 END) AS missing_poverty_rows,
            1.0 - (
                SUM(CASE WHEN p.county_fips IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
            ) AS match_rate
        FROM contract_county_weights w
        LEFT JOIN county_poverty p
            ON p.county_fips = w.county_fips
           AND p.year = w.year - 1
        GROUP BY 1
        ORDER BY 1
    """).df().to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()