"""
05_build_weights.py

Builds contract-county enrollment weights used across the pipeline.

Inputs (CSV)
- data_staged/enrollment/enrollment_contract_county_2024_01.csv
- data_staged/enrollment/enrollment_contract_county_2025_01.csv

Outputs (DuckDB: db/ma_stars.duckdb)
- View:  enrollment_union
- Table: contract_county_weights
  (contract_id, year, county_fips, enrollment, contract_year_total_enrollment, w_enroll)

QC
- Prints row counts by year for the unioned view
- Prints weight-sum stats (weights should sum to ~1 within contract-year)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

CSV_2024 = BASE_DIR / "data_staged" / "enrollment" / "enrollment_contract_county_2024_01.csv"
CSV_2025 = BASE_DIR / "data_staged" / "enrollment" / "enrollment_contract_county_2025_01.csv"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")
    if not CSV_2024.exists():
        die(f"Missing input file: {CSV_2024}")
    if not CSV_2025.exists():
        die(f"Missing input file: {CSV_2025}")

    con = duckdb.connect(str(DB_PATH))

    # --- 1) Union both years into a canonical view ---
    con.execute(f"""
        CREATE OR REPLACE VIEW enrollment_union AS
        SELECT
            contract_id,
            CAST(year AS INTEGER) AS year,
            LPAD(CAST(county_fips AS VARCHAR), 5, '0') AS county_fips,
            CAST(enrollment AS BIGINT) AS enrollment
        FROM read_csv_auto('{CSV_2024.as_posix()}')
        UNION ALL
        SELECT
            contract_id,
            CAST(year AS INTEGER) AS year,
            LPAD(CAST(county_fips AS VARCHAR), 5, '0') AS county_fips,
            CAST(enrollment AS BIGINT) AS enrollment
        FROM read_csv_auto('{CSV_2025.as_posix()}')
    """)

    counts = con.execute("""
        SELECT year, COUNT(*) AS rows
        FROM enrollment_union
        GROUP BY 1
        ORDER BY 1
    """).df()
    print("\nEnrollment union row counts:")
    print(counts.to_string(index=False))

    # --- 2) Materialize weights table ---
    con.execute("""
        CREATE OR REPLACE TABLE contract_county_weights AS
        SELECT
            contract_id,
            year,
            county_fips,
            enrollment,
            SUM(enrollment) OVER (PARTITION BY contract_id, year) AS contract_year_total_enrollment,
            enrollment * 1.0
              / SUM(enrollment) OVER (PARTITION BY contract_id, year) AS w_enroll
        FROM enrollment_union
        WHERE enrollment IS NOT NULL AND enrollment > 0
    """)

    # --- 3) QC: weights should sum to ~1 within each contract-year ---
    qa = con.execute("""
        SELECT
            year,
            COUNT(*) AS contract_years_checked,
            AVG(wsum) AS avg_weight_sum,
            MIN(wsum) AS min_weight_sum,
            MAX(wsum) AS max_weight_sum
        FROM (
            SELECT contract_id, year, SUM(w_enroll) AS wsum
            FROM contract_county_weights
            GROUP BY 1, 2
        )
        GROUP BY 1
        ORDER BY 1
    """).df()

    print("\nWeight-sum QC (should be ~1.0):")
    print(qa.to_string(index=False))

    print("\nDuckDB tables:")
    print(con.execute("SHOW TABLES").df().to_string(index=False))


if __name__ == "__main__":
    main()