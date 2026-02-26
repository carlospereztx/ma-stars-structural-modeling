"""
01_stage_stars.py

Stages CMS Medicare Advantage contract-level Star Ratings (overall) for 2024 and 2025.

Inputs
- data_raw/stars_2024/2024 Star Ratings Data Table - Summary Rating (Jul 2 2024).csv
- data_raw/stars_2025/2025 Star Ratings Data Table - Summary Ratings (Dec 2 2024).csv

Outputs
- data_staged/stars/stars_contract_2024.csv
- data_staged/stars/stars_contract_2025.csv

Notes
- Source files include a banner row; this script skips the first row to read the real header.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_2024 = PROJECT_ROOT / "data_raw" / "stars_2024" / "2024 Star Ratings Data Table - Summary Rating (Jul 2 2024).csv"
RAW_2025 = PROJECT_ROOT / "data_raw" / "stars_2025" / "2025 Star Ratings Data Table - Summary Ratings (Dec 2 2024).csv"

OUT_DIR = PROJECT_ROOT / "data_staged" / "stars"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def stage_summary_rating(csv_path: Path, year: int, overall_col: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing input file: {csv_path}")

    # Source files include a banner row above the header.
    df = pd.read_csv(csv_path, skiprows=1, dtype=str)

    # Trim whitespace in string columns.
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    keep_cols = ["Contract Number", overall_col]
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}. Found columns: {list(df.columns)}")

    out = (
        df[keep_cols]
        .rename(columns={
            "Contract Number": "contract_id",
            overall_col: "stars_overall",
        })
        .copy()
    )

    out["year"] = year

    out["contract_id"] = out["contract_id"].astype(str).str.strip().str.upper()
    out["stars_overall"] = pd.to_numeric(out["stars_overall"], errors="coerce")

    # Drop rows without a contract ID.
    out = out.dropna(subset=["contract_id"])

    return out[["contract_id", "year", "stars_overall"]]


def main() -> None:
    df_2024 = stage_summary_rating(RAW_2024, 2024, "2024 Overall")
    df_2024.to_csv(OUT_DIR / "stars_contract_2024.csv", index=False)
    print(f"Saved {len(df_2024):,} rows -> stars_contract_2024.csv")

    df_2025 = stage_summary_rating(RAW_2025, 2025, "2025 Overall")
    df_2025.to_csv(OUT_DIR / "stars_contract_2025.csv", index=False)
    print(f"Saved {len(df_2025):,} rows -> stars_contract_2025.csv")


if __name__ == "__main__":
    main()