"""
13_threshold_logit_structural.py

Inputs
- DuckDB database at DB_PATH
- main.contract_year_near_threshold_structural
  - above_4star (binary)
  - poverty_exposure
  - rural_exposure
  - contract_year
  - total_enrollment (for weighting)

Outputs
- Prints model summaries to stdout:
  - Unweighted logistic regression (HC1 robust SE)
  - Enrollment-weighted logistic regression (HC1 robust SE)
  - Odds ratios for poverty_exposure and rural_exposure

Notes
- Uses GLM Binomial with year fixed effects via C(contract_year).
- Weighted model uses total_enrollment as freq_weights.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"

IN_TBL = "main.contract_year_near_threshold_structural"

FORMULA = "above_4star ~ poverty_exposure + rural_exposure + C(contract_year)"
ROBUST_COV = "HC1"


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


def odds_ratio(model, term: str) -> float:
    if term not in model.params:
        die(f"Model term not found: {term}")
    return float(np.exp(model.params[term]))


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    try:
        require_table(con, IN_TBL)

        df = con.execute(f"SELECT * FROM {IN_TBL}").df()
        print("\nDataset shape:", df.shape)

        required = ["above_4star", "poverty_exposure", "rural_exposure", "contract_year", "total_enrollment"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            die(f"Missing required columns in {IN_TBL}: {missing}")

        # Unweighted model
        print("\n--- Logistic regression (contract-level) ---")
        model_unw = smf.glm(
            formula=FORMULA,
            data=df,
            family=sm.families.Binomial(),
        ).fit(cov_type=ROBUST_COV)

        print(model_unw.summary())
        print("\nUnweighted OR per +1 poverty point:", round(odds_ratio(model_unw, "poverty_exposure"), 4))
        print("Unweighted OR per +1 rural exposure unit:", round(odds_ratio(model_unw, "rural_exposure"), 4))

        # Enrollment-weighted model
        print("\n--- Logistic regression (enrollment-weighted) ---")
        model_w = smf.glm(
            formula=FORMULA,
            data=df,
            family=sm.families.Binomial(),
            freq_weights=df["total_enrollment"],
        ).fit(cov_type=ROBUST_COV)

        print(model_w.summary())
        print("\nEnrollment-weighted OR per +1 poverty point:", round(odds_ratio(model_w, "poverty_exposure"), 4))
        print("Enrollment-weighted OR per +1 rural exposure unit:", round(odds_ratio(model_w, "rural_exposure"), 4))

    except Exception as e:
        die(f"Structural logistic regression failed: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()