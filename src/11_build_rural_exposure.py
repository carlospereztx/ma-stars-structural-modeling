"""
11_build_rural_exposure.py

Inputs
- DuckDB database at DB_PATH
- main.contract_county_weights
- main.county_rucc_2023

Outputs
- main.contract_year_rural_exposure
  - rural_exposure: enrollment-weighted share of enrollment in rural counties (RUCC>=4 => rural_indicator=1)
  - coverage_weight_share: share of weights with RUCC coverage
  - out_of_scope_geography: 1 if no RUCC-matched weights for that contract-year

Notes
- Uses only RUCC-covered weights in the exposure denominator (avoids penalizing missing RUCC matches).
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

WEIGHTS_TBL = "main.contract_county_weights"
RUCC_TBL = "main.county_rucc_2023"
OUT_TBL = "main.contract_year_rural_exposure"


def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(code)


def require_table(con: duckdb.DuckDBPyConnection, full_name: str) -> None:
    schema, name = full_name.split(".", 1)
    n = con.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.tables
        WHERE lower(table_schema) = lower(?)
          AND lower(table_name) = lower(?)
        """,
        [schema, name],
    ).fetchone()[0]
    if n == 0:
        die(f"Required table missing: {full_name}")


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    try:
        require_table(con, WEIGHTS_TBL)
        require_table(con, RUCC_TBL)

        con.execute(
            f"""
            CREATE OR REPLACE TABLE {OUT_TBL} AS
            WITH joined AS (
              SELECT
                w.contract_id,
                w.year AS contract_year,
                w.contract_year_total_enrollment,
                w.w_enroll,
                r.rural_indicator
              FROM {WEIGHTS_TBL} w
              LEFT JOIN {RUCC_TBL} r
                ON r.county_fips = w.county_fips
            ),
            agg AS (
              SELECT
                contract_id,
                contract_year,
                MAX(contract_year_total_enrollment) AS total_enrollment,
                SUM(CASE WHEN rural_indicator IS NOT NULL THEN w_enroll * rural_indicator ELSE 0 END) AS num,
                SUM(CASE WHEN rural_indicator IS NOT NULL THEN w_enroll ELSE 0 END) AS denom
              FROM joined
              GROUP BY 1,2
            )
            SELECT
              contract_id,
              contract_year,
              total_enrollment,
              CASE WHEN denom > 0 THEN num / denom ELSE NULL END AS rural_exposure,
              denom AS coverage_weight_share,
              CASE WHEN denom = 0 THEN 1 ELSE 0 END AS out_of_scope_geography
            FROM agg
            """
        )

        print("\nSample rural exposures (most rural):")
        print(
            con.execute(
                f"""
                SELECT *
                FROM {OUT_TBL}
                WHERE out_of_scope_geography = 0
                ORDER BY rural_exposure DESC
                LIMIT 10
                """
            ).df()
        )

        print("\nCoverage summary:")
        print(
            con.execute(
                f"""
                SELECT
                  contract_year,
                  COUNT(*) AS contract_years,
                  AVG(coverage_weight_share) AS avg_coverage,
                  MIN(coverage_weight_share) AS min_coverage,
                  MAX(coverage_weight_share) AS max_coverage,
                  SUM(out_of_scope_geography) AS out_of_scope_count
                FROM {OUT_TBL}
                GROUP BY 1
                ORDER BY 1
                """
            ).df()
        )

    except Exception as e:
        die(f"Rural exposure build failed: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()