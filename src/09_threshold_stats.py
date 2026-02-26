"""
09_threshold_stats.py

Runs basic inference for the near-threshold sample:
- Bootstrap CI for poverty exposure differences (3.5–3.9 minus 4.0–4.5)
- Logistic regression for above_4star ~ poverty_exposure + year fixed effects
  (unweighted and enrollment-weighted via freq_weights)

Inputs (DuckDB)
- contract_year_near_threshold

Output
- Prints stats tables and model summaries to stdout
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

try:
    import statsmodels.api as sm
    import statsmodels.formula.api as smf
except ImportError as e:
    raise SystemExit("Missing dependency: statsmodels. Install it and rerun.") from e


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "ma_stars.duckdb"
SOURCE_TABLE = "contract_year_near_threshold"

N_BOOT = 2000
RNG_SEED = 42


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def weighted_mean(x: np.ndarray, w: np.ndarray) -> float:
    wsum = float(np.sum(w))
    if wsum == 0:
        return float("nan")
    return float(np.sum(x * w) / wsum)


def bootstrap_diff(df: pd.DataFrame, n_boot: int, seed: int) -> pd.DataFrame:
    """
    Bootstrap within each contract_year by resampling contract_id with replacement.
    For each replicate, compute:
      - unweighted diff in mean poverty exposure
      - enrollment-weighted diff in mean poverty exposure
    Diff is (3.5–3.9) minus (4.0–4.5).
    """
    rng = np.random.default_rng(seed)

    years = sorted(df["contract_year"].unique())
    out_rows: list[dict] = []

    for yr in years:
        d_yr = df[df["contract_year"] == yr].copy()

        d_a = d_yr[d_yr["threshold_band"] == "3.5-3.9"].copy()
        d_b = d_yr[d_yr["threshold_band"] == "4.0-4.5"].copy()

        ids_a = d_a["contract_id"].dropna().unique()
        ids_b = d_b["contract_id"].dropna().unique()

        if len(ids_a) < 5 or len(ids_b) < 5:
            print(f"WARNING: small sample in {yr}. Skipping bootstrap for that year.")
            continue

        for _ in range(n_boot):
            samp_a = rng.choice(ids_a, size=len(ids_a), replace=True)
            samp_b = rng.choice(ids_b, size=len(ids_b), replace=True)

            # Preserve multiplicity by expanding sampled IDs and joining back
            bs_a = pd.DataFrame({"contract_id": samp_a}).merge(d_a, on="contract_id", how="left")
            bs_b = pd.DataFrame({"contract_id": samp_b}).merge(d_b, on="contract_id", how="left")

            mu_a = bs_a["poverty_exposure"].mean()
            mu_b = bs_b["poverty_exposure"].mean()
            diff_unw = float(mu_a - mu_b)

            ew_a = weighted_mean(bs_a["poverty_exposure"].to_numpy(), bs_a["total_enrollment"].to_numpy())
            ew_b = weighted_mean(bs_b["poverty_exposure"].to_numpy(), bs_b["total_enrollment"].to_numpy())
            diff_w = float(ew_a - ew_b)

            out_rows.append({
                "contract_year": yr,
                "diff_unweighted": diff_unw,
                "diff_enroll_weighted": diff_w,
            })

    return pd.DataFrame(out_rows)


def summarize_boot(df_boot: pd.DataFrame, col: str) -> pd.DataFrame:
    """Mean + 95% percentile CI by year for the given bootstrap column."""
    def pct(x, p):
        return float(np.nanpercentile(x, p))

    return (
        df_boot.groupby("contract_year")[col]
        .agg(
            boot_mean="mean",
            ci2p5=lambda x: pct(x, 2.5),
            ci50=lambda x: pct(x, 50),
            ci97p5=lambda x: pct(x, 97.5),
        )
        .reset_index()
    )


def run_logit_models(df: pd.DataFrame) -> None:
    """
    Logistic regression within near-threshold sample:
      above_4star ~ poverty_exposure + C(contract_year)
    Runs:
      - unweighted (contract-level)
      - enrollment-weighted via freq_weights
    """
    d = df.copy()
    d["contract_year"] = d["contract_year"].astype(int)
    d["above_4star"] = d["above_4star"].astype(int)

    formula = "above_4star ~ poverty_exposure + C(contract_year)"

    print("\n--- Logistic regression (unweighted) ---")
    m1 = smf.glm(formula=formula, data=d, family=sm.families.Binomial()).fit(cov_type="HC1")
    print(m1.summary())

    b = float(m1.params["poverty_exposure"])
    se = float(m1.bse["poverty_exposure"])
    or_ = float(np.exp(b))
    ci_lo = float(np.exp(b - 1.96 * se))
    ci_hi = float(np.exp(b + 1.96 * se))
    print(f"\nUnweighted OR per +1 poverty point: {or_:.4f} (95% CI {ci_lo:.4f}, {ci_hi:.4f})")

    print("\n--- Logistic regression (enrollment-weighted via freq_weights) ---")
    m2 = smf.glm(
        formula=formula,
        data=d,
        family=sm.families.Binomial(),
        freq_weights=d["total_enrollment"],
    ).fit(cov_type="HC1")
    print(m2.summary())

    b2 = float(m2.params["poverty_exposure"])
    se2 = float(m2.bse["poverty_exposure"])
    or2 = float(np.exp(b2))
    ci2_lo = float(np.exp(b2 - 1.96 * se2))
    ci2_hi = float(np.exp(b2 + 1.96 * se2))
    print(f"\nEnrollment-weighted OR per +1 poverty point: {or2:.4f} (95% CI {ci2_lo:.4f}, {ci2_hi:.4f})")


def main() -> None:
    if not DB_PATH.exists():
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if SOURCE_TABLE not in tables:
        die(f"Missing required table: {SOURCE_TABLE}. Run src/08_build_near_threshold_analysis.py first.")

    df = con.execute(f"""
        SELECT
            contract_id,
            contract_year,
            threshold_band,
            above_4star,
            poverty_exposure,
            total_enrollment
        FROM {SOURCE_TABLE}
        WHERE threshold_band IN ('3.5-3.9', '4.0-4.5')
    """).df()

    print("\nNear-threshold dataset size (by year/band):")
    size_tbl = (
        df.groupby(["contract_year", "threshold_band"])
        .agg(
            contracts=("contract_id", "nunique"),
            rows=("contract_id", "size"),
            total_enrollment=("total_enrollment", "sum"),
        )
        .reset_index()
        .sort_values(["contract_year", "threshold_band"])
    )
    print(size_tbl.to_string(index=False))

    print("\nPoint estimates (means):")
    pt = (
        df.groupby(["contract_year", "threshold_band"])
        .apply(lambda g: pd.Series({
            "mean_unweighted": g["poverty_exposure"].mean(),
            "mean_enroll_weighted": weighted_mean(
                g["poverty_exposure"].to_numpy(),
                g["total_enrollment"].to_numpy(),
            ),
        }))
        .reset_index()
        .sort_values(["contract_year", "threshold_band"])
    )
    print(pt.to_string(index=False))

    print(f"\nBootstrapping differences (N_BOOT={N_BOOT}) ...")
    df_boot = bootstrap_diff(df, n_boot=N_BOOT, seed=RNG_SEED)
    if df_boot.empty:
        die("Bootstrap returned empty results. Check dataset sizes.")

    summ_unw = summarize_boot(df_boot, "diff_unweighted")
    summ_w = summarize_boot(df_boot, "diff_enroll_weighted")

    print("\nBootstrap CI: diff (3.5–3.9 minus 4.0–4.5) — UNWEIGHTED")
    print(summ_unw.to_string(index=False))

    print("\nBootstrap CI: diff (3.5–3.9 minus 4.0–4.5) — ENROLLMENT-WEIGHTED")
    print(summ_w.to_string(index=False))

    run_logit_models(df)

    print("\nDone.")


if __name__ == "__main__":
    main()