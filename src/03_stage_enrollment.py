"""
03_stage_enrollment.py

Stages CMS contract-county enrollment extracts for 2024-01 and 2025-01.

Inputs
- data_raw/2024_01/CPSC_Enrollment_Info_2024_01.csv
- data_raw/2025_01/CPSC_Enrollment_Info_2025_01.csv

Outputs
- data_staged/enrollment/enrollment_contract_county_2024_01.csv
- data_staged/enrollment/enrollment_contract_county_2025_01.csv

Notes
- Enrollment values may be numeric text, '*' (<=10), or blank (suppressed).
  This staging step treats '*' and blanks as 0.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_2024 = PROJECT_ROOT / "data_raw" / "2024_01" / "CPSC_Enrollment_Info_2024_01.csv"
RAW_2025 = PROJECT_ROOT / "data_raw" / "2025_01" / "CPSC_Enrollment_Info_2025_01.csv"

OUT_DIR = PROJECT_ROOT / "data_staged" / "enrollment"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def clean_enrollment_value(x) -> int:
    """Parse enrollment values; treat '*' and blanks as 0."""
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if s in ("", "*"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def stage_file(path: Path, year: int) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    print(f"Reading {path.name} ...")
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Handle CMS column name variants
    contract_col = pick_col(df, ["Contract ID", "Contract Number"])
    fips_col = pick_col(df, ["FIPS Code", "FIPS State County Code"])
    enroll_col = pick_col(df, ["Enrollment"])

    missing = []
    if contract_col is None:
        missing.append("Contract ID/Contract Number")
    if fips_col is None:
        missing.append("FIPS Code/FIPS State County Code")
    if enroll_col is None:
        missing.append("Enrollment")

    if missing:
        raise ValueError(
            f"Missing required columns in {path.name}: {missing}\nFound: {list(df.columns)}"
        )

    out = (
        df[[contract_col, fips_col, enroll_col]]
        .rename(columns={
            contract_col: "contract_id",
            fips_col: "county_fips_raw",
            enroll_col: "enrollment_raw",
        })
        .copy()
    )

    out["contract_id"] = out["contract_id"].astype(str).str.upper().str.strip()

    # Keep digits only, take last 5, zero-pad (county FIPS)
    out["county_fips"] = (
        out["county_fips_raw"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str[-5:]
        .str.zfill(5)
    )

    out["enrollment"] = out["enrollment_raw"].apply(clean_enrollment_value).astype(int)
    out["year"] = year

    # Aggregate plan-level rows into contract-county-year totals
    out = (
        out.groupby(["contract_id", "year", "county_fips"], as_index=False)["enrollment"]
        .sum()
    )

    # Keep only valid 5-digit FIPS
    out = out[out["county_fips"].str.len() == 5].copy()

    return out


def main() -> None:
    df_2024 = stage_file(RAW_2024, 2024)
    df_2024.to_csv(OUT_DIR / "enrollment_contract_county_2024_01.csv", index=False)
    print(f"Saved {len(df_2024):,} rows -> enrollment_contract_county_2024_01.csv")

    df_2025 = stage_file(RAW_2025, 2025)
    df_2025.to_csv(OUT_DIR / "enrollment_contract_county_2025_01.csv", index=False)
    print(f"Saved {len(df_2025):,} rows -> enrollment_contract_county_2025_01.csv")


if __name__ == "__main__":
    main()