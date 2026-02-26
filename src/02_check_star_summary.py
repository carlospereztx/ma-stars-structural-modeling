"""
02_check_star_summary.py

Quick QC summary for staged contract-level Star Ratings.

Inputs
- data_staged/stars/stars_contract_2024.csv
- data_staged/stars/stars_contract_2025.csv

Output
- Prints basic completeness stats to stdout
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGED_DIR = PROJECT_ROOT / "data_staged" / "stars"

F_2024 = STAGED_DIR / "stars_contract_2024.csv"
F_2025 = STAGED_DIR / "stars_contract_2025.csv"


def summarize(df: pd.DataFrame, year: int) -> None:
    required = {"contract_id", "year", "stars_overall"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"{year} file missing columns: {missing_cols}")

    total = len(df)
    numeric = df["stars_overall"].notna().sum()
    missing = df["stars_overall"].isna().sum()
    pct_numeric = (numeric / total) if total > 0 else 0.0

    print(f"\n===== {year} =====")
    print(f"Total rows:       {total:,}")
    print(f"Non-missing stars:{numeric:,}")
    print(f"Missing stars:    {missing:,}")
    print(f"% non-missing:    {pct_numeric:.2%}")


def main() -> None:
    if not F_2024.exists():
        raise FileNotFoundError(f"Missing input file: {F_2024}")
    if not F_2025.exists():
        raise FileNotFoundError(f"Missing input file: {F_2025}")

    df_2024 = pd.read_csv(F_2024)
    df_2025 = pd.read_csv(F_2025)

    summarize(df_2024, 2024)
    summarize(df_2025, 2025)


if __name__ == "__main__":
    main()