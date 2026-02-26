"""
10_stage_rural_rucc.py

Inputs
- DuckDB database at DB_PATH
- RUCC 2023 source CSV at RUCC_PATH (long format with FIPS, State, County_Name, Attribute, Value)

Outputs
- DuckDB view: stage.rucc_long_raw
- DuckDB table: stage.county_rucc_2023
- Optional export: data_staged/structural/county_rucc_2023.csv

Notes
- RUCC 2023 rural indicator convention: rucc_2023 >= 4 => rural_indicator = 1, else 0.
- Keeps the source CSV encoding as latin-1 for compatibility with RUCC file formatting.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


# -----------------------------
# Paths / constants
# -----------------------------

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

RUCC_PATH = BASE_DIR / "data_raw" / "rucc" / "Ruralurbancontinuumcodes2023.csv"
OUT_DIR = BASE_DIR / "data_staged" / "structural"

STAGE_SCHEMA = "stage"
RAW_VIEW = f"{STAGE_SCHEMA}.rucc_long_raw"
OUT_TABLE = f"{STAGE_SCHEMA}.county_rucc_2023"


# -----------------------------
# Utilities
# -----------------------------

def die(msg: str, code: int = 1) -> None:
    """Print an error message and exit with a non-zero status."""
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(code)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection."""
    try:
        return duckdb.connect(str(db_path))
    except Exception as e:
        die(f"Failed to connect to DuckDB at {db_path}: {e}")
        raise  # unreachable


# -----------------------------
# Core logic
# -----------------------------

def stage_rucc(con: duckdb.DuckDBPyConnection, rucc_path: Path) -> None:
    """Create raw RUCC view and staged county RUCC table."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGE_SCHEMA};")

    # 1) Raw long view
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {RAW_VIEW} AS
        SELECT
          LPAD(CAST(FIPS AS VARCHAR), 5, '0') AS county_fips,
          State AS state,
          County_Name AS county_name,
          Attribute AS attribute,
          Value AS value
        FROM read_csv_auto('{rucc_path.as_posix()}', encoding='latin-1')
        """
    )

    # 2) Pivot to county-level RUCC_2023 and optional population
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {OUT_TABLE} AS
        WITH base AS (
          SELECT
            county_fips,
            ANY_VALUE(state) AS state,
            ANY_VALUE(county_name) AS county_name,
            MAX(CASE WHEN attribute = 'RUCC_2023' THEN TRY_CAST(value AS INTEGER) END) AS rucc_2023,
            MAX(CASE WHEN attribute = 'Population_2020' THEN TRY_CAST(value AS BIGINT) END) AS population_2020
          FROM {RAW_VIEW}
          GROUP BY 1
        )
        SELECT
          county_fips,
          state,
          county_name,
          rucc_2023,
          population_2020,
          CASE
            WHEN rucc_2023 IS NULL THEN NULL
            WHEN rucc_2023 >= 4 THEN 1
            ELSE 0
          END AS rural_indicator
        FROM base
        """
    )


def qa(con: duckdb.DuckDBPyConnection) -> None:
    """Run lightweight QA checks and print to stdout."""
    print("\nCounty RUCC rows:")
    print(con.execute(f"SELECT COUNT(*) AS rows FROM {OUT_TABLE}").df())

    print("\nRUCC missing check:")
    print(
        con.execute(
            f"""
            SELECT
              SUM(CASE WHEN rucc_2023 IS NULL THEN 1 ELSE 0 END) AS missing_rucc,
              SUM(CASE WHEN rural_indicator IS NULL THEN 1 ELSE 0 END) AS missing_rural_indicator
            FROM {OUT_TABLE}
            """
        ).df()
    )

    print("\nRUCC distribution:")
    print(
        con.execute(
            f"""
            SELECT rucc_2023, COUNT(*) AS counties
            FROM {OUT_TABLE}
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
    )


def export_csv(con: duckdb.DuckDBPyConnection, out_dir: Path) -> Path:
    """Export staged RUCC table to CSV for transparency."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "county_rucc_2023.csv"

    con.execute(
        f"""
        COPY (SELECT * FROM {OUT_TABLE})
        TO '{out_path.as_posix()}' (HEADER, DELIMITER ',');
        """
    )
    return out_path


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB not found: {DB_PATH}")

    if not RUCC_PATH.exists():
        die(f"RUCC source file not found: {RUCC_PATH}")

    con = connect(DB_PATH)
    try:
        stage_rucc(con, RUCC_PATH)
        qa(con)
        out_path = export_csv(con, OUT_DIR)
        print(f"\nSaved staged RUCC table -> {out_path}")
    except Exception as e:
        die(f"RUCC staging failed: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()