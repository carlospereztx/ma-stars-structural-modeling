"""
04_inspect_enrollment.py

Quick schema check for a staged enrollment CSV using DuckDB.

Input
- data_staged/enrollment/enrollment_contract_county_2024_01.csv

Output
- Prints inferred schema to stdout
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"
CSV_PATH = BASE_DIR / "data_staged" / "enrollment" / "enrollment_contract_county_2024_01.csv"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")
    if not CSV_PATH.exists():
        die(f"CSV not found at: {CSV_PATH}")

    con = duckdb.connect(str(DB_PATH))

    schema = con.execute(f"""
        DESCRIBE
        SELECT *
        FROM read_csv_auto('{CSV_PATH.as_posix()}')
    """).df()

    print(schema.to_string(index=False))


if __name__ == "__main__":
    main()