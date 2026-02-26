"""
12_build_threshold_with_rural.py

Inputs
- DuckDB database at DB_PATH
- main.contract_year_near_threshold
- main.contract_year_poverty_exposure
- main.contract_year_rural_exposure

Outputs
- main.contract_year_near_threshold_structural
  - Near-threshold contract-years (threshold_band in {'3.5-3.9','4.0-4.5'})
  - Adds structural exposure features (poverty_exposure, rural_exposure)

Notes
- Exposures are left-joined; NULLs indicate missing structural coverage upstream.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

NEAR_TBL = "main.contract_year_near_threshold"
POV_TBL = "main.contract_year_poverty_exposure"
RURAL_TBL = "main.contract_year_rural_exposure"
OUT_TBL = "main.contract_year_near_threshold_structural"


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
        require_table(con, NEAR_TBL)
        require_table(con, POV_TBL)
        require_table(con, RURAL_TBL)

        con.execute(
            f"""
            CREATE OR REPLACE TABLE {OUT_TBL} AS
            SELECT
              n.contract_id,
              n.contract_year,
              n.stars_rating,
              n.threshold_band,
              n.above_4star,
              n.total_enrollment,
              p.poverty_exposure,
              r.rural_exposure
            FROM {NEAR_TBL} n
            LEFT JOIN {POV_TBL} p
              ON p.contract_id = n.contract_id
             AND p.contract_year = n.contract_year
            LEFT JOIN {RURAL_TBL} r
              ON r.contract_id = n.contract_id
             AND r.contract_year = n.contract_year
            WHERE n.threshold_band IN ('3.5-3.9', '4.0-4.5')
            """
        )

        print("\nSample structural dataset:")
        print(con.execute(f"SELECT * FROM {OUT_TBL} LIMIT 10").df())

        print("\nMissing exposure check:")
        print(
            con.execute(
                f"""
                SELECT
                  SUM(CASE WHEN poverty_exposure IS NULL THEN 1 ELSE 0 END) AS missing_poverty,
                  SUM(CASE WHEN rural_exposure IS NULL THEN 1 ELSE 0 END) AS missing_rural
                FROM {OUT_TBL}
                """
            ).df()
        )

    except Exception as e:
        die(f"Near-threshold structural build failed: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()