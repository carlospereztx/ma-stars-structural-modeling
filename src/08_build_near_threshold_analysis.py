"""
08_build_near_threshold_analysis.py

Builds a near-threshold analysis dataset by joining contract Stars to poverty exposure.

Inputs
- data_staged/stars/stars_contract_2024.csv
- data_staged/stars/stars_contract_2025.csv
- DuckDB table: contract_year_poverty_exposure_clean

Outputs (DuckDB)
- contract_year_stars
- contract_year_analysis_base
- contract_year_near_threshold

QC
- Prints Stars range checks
- Prints near-threshold counts and poverty summaries
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

STARS_2024_PATH = BASE_DIR / "data_staged" / "stars" / "stars_contract_2024.csv"
STARS_2025_PATH = BASE_DIR / "data_staged" / "stars" / "stars_contract_2025.csv"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def describe_csv(con: duckdb.DuckDBPyConnection, path: Path):
    return con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{path.as_posix()}')").df()


def pick_stars_col(cols: list[str]) -> str:
    candidates = [
        "stars_overall",
        "stars",
        "star_rating",
        "overall_star_rating",
        "overall_stars",
        "overall_rating",
        "summary_star_rating",
    ]
    for c in candidates:
        if c in cols:
            return c
    raise ValueError(
        f"Could not find a stars column. Available columns: {cols}. "
        "Update candidate list if needed."
    )


def load_and_standardize_stars(con: duckdb.DuckDBPyConnection) -> None:
    if not STARS_2024_PATH.exists():
        die(f"Missing input file: {STARS_2024_PATH}")
    if not STARS_2025_PATH.exists():
        die(f"Missing input file: {STARS_2025_PATH}")

    print("\nDESCRIBE stars 2024:")
    print(describe_csv(con, STARS_2024_PATH).to_string(index=False))
    print("\nDESCRIBE stars 2025:")
    print(describe_csv(con, STARS_2025_PATH).to_string(index=False))

    con.execute(f"""
        CREATE OR REPLACE VIEW stars_raw_2024 AS
        SELECT * FROM read_csv_auto('{STARS_2024_PATH.as_posix()}')
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW stars_raw_2025 AS
        SELECT * FROM read_csv_auto('{STARS_2025_PATH.as_posix()}')
    """)

    cols_2024 = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'stars_raw_2024'
        ORDER BY ordinal_position
    """).df()["column_name"].tolist()

    cols_2025 = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'stars_raw_2025'
        ORDER BY ordinal_position
    """).df()["column_name"].tolist()

    stars_col_2024 = pick_stars_col(cols_2024)
    stars_col_2025 = pick_stars_col(cols_2025)

    print(f"\nUsing stars column for 2024: {stars_col_2024}")
    print(f"Using stars column for 2025: {stars_col_2025}")

    con.execute(f"""
        CREATE OR REPLACE TABLE contract_year_stars AS
        SELECT
            contract_id,
            2024 AS contract_year,
            CAST({stars_col_2024} AS DOUBLE) AS stars_rating
        FROM stars_raw_2024
        UNION ALL
        SELECT
            contract_id,
            2025 AS contract_year,
            CAST({stars_col_2025} AS DOUBLE) AS stars_rating
        FROM stars_raw_2025
    """)

    print("\ncontract_year_stars sample:")
    print(con.execute("SELECT * FROM contract_year_stars LIMIT 10").df().to_string(index=False))

    print("\nStars range check:")
    print(con.execute("""
        SELECT
            contract_year,
            MIN(stars_rating) AS min_stars,
            MAX(stars_rating) AS max_stars,
            COUNT(*) AS rows
        FROM contract_year_stars
        GROUP BY 1
        ORDER BY 1
    """).df().to_string(index=False))


def build_near_threshold_dataset(con: duckdb.DuckDBPyConnection) -> None:
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for t in ["contract_year_stars", "contract_year_poverty_exposure_clean"]:
        if t not in tables:
            die(f"Required table missing: {t}")

    con.execute("""
        CREATE OR REPLACE TABLE contract_year_analysis_base AS
        SELECT
            s.contract_id,
            s.contract_year,
            s.stars_rating,
            p.poverty_exposure,
            p.coverage_weight_share,
            p.out_of_scope_geography,
            p.total_enrollment
        FROM contract_year_stars s
        LEFT JOIN contract_year_poverty_exposure_clean p
            ON p.contract_id = s.contract_id
           AND p.contract_year = s.contract_year
    """)

    print("\nBase join sample:")
    print(con.execute("""
        SELECT *
        FROM contract_year_analysis_base
        LIMIT 10
    """).df().to_string(index=False))

    con.execute("""
        CREATE OR REPLACE TABLE contract_year_near_threshold AS
        SELECT
            *,
            CASE WHEN stars_rating >= 4.0 THEN 1 ELSE 0 END AS above_4star,
            CASE
                WHEN stars_rating >= 3.5 AND stars_rating < 4.0 THEN '3.5-3.9'
                WHEN stars_rating >= 4.0 AND stars_rating <= 4.5 THEN '4.0-4.5'
                ELSE 'other'
            END AS threshold_band
        FROM contract_year_analysis_base
        WHERE stars_rating IS NOT NULL
          AND poverty_exposure IS NOT NULL
          AND out_of_scope_geography = 0
          AND stars_rating >= 3.5
          AND stars_rating <= 4.5
    """)

    print("\nNear-threshold counts by year and band:")
    print(con.execute("""
        SELECT
            contract_year,
            threshold_band,
            COUNT(*) AS contracts,
            SUM(total_enrollment) AS total_enrollment
        FROM contract_year_near_threshold
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df().to_string(index=False))

    print("\nPoverty exposure by band (unweighted + enrollment-weighted):")
    print(con.execute("""
        SELECT
            contract_year,
            threshold_band,
            AVG(poverty_exposure) AS mean_poverty_exposure,
            SUM(poverty_exposure * total_enrollment) / NULLIF(SUM(total_enrollment), 0)
                AS enroll_weighted_poverty_exposure
        FROM contract_year_near_threshold
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df().to_string(index=False))

    print("\nDifference: 3.5-3.9 minus 4.0-4.5 (enrollment-weighted):")
    print(con.execute("""
        WITH agg AS (
            SELECT
                contract_year,
                threshold_band,
                SUM(poverty_exposure * total_enrollment) / NULLIF(SUM(total_enrollment), 0) AS ew_mean
            FROM contract_year_near_threshold
            GROUP BY 1, 2
        )
        SELECT
            a.contract_year,
            a.ew_mean AS ew_mean_35_39,
            b.ew_mean AS ew_mean_40_45,
            (a.ew_mean - b.ew_mean) AS diff_35_39_minus_40_45
        FROM agg a
        JOIN agg b
            ON a.contract_year = b.contract_year
        WHERE a.threshold_band = '3.5-3.9'
          AND b.threshold_band = '4.0-4.5'
        ORDER BY a.contract_year
    """).df().to_string(index=False))

    print("\nNear-threshold dataset sample:")
    print(con.execute("""
        SELECT contract_id, contract_year, stars_rating, threshold_band, poverty_exposure, total_enrollment
        FROM contract_year_near_threshold
        ORDER BY contract_year, stars_rating
        LIMIT 20
    """).df().to_string(index=False))


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    load_and_standardize_stars(con)
    build_near_threshold_dataset(con)

    print("\nDone.")


if __name__ == "__main__":
    main()