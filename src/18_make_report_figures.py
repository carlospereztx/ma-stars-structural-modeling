"""
18_make_report_figures.py

Creates report figures for the MA Stars structural decomposition memo.

Reads CSV artifacts created by 17_export_report_artifacts.py and saves PNGs to:
  reports/figures/

Figures (PNG)
1) fig01_structural_vs_observed_scatter.png
   - Expected structural Stars (x) vs observed Stars (y)
   - 45-degree reference line
   - Highlights near-threshold contracts (3.0–3.9)

2) fig02_residuals_by_star_band.png
   - Boxplot of operational residuals by star band: 3.0–3.4, 3.5–3.9, 4.0+

3) fig03_hpsa_decile_gradient.png
   - Mean expected Stars across HPSA exposure deciles (by year)

4) fig04_scale_decile_gradient.png
   - Mean expected Stars across scale (log enrollment) deciles (by year)

5) fig05_opportunity_table_top15.png
   - Image table of the top 15 opportunities in 3.5–3.9 (most negative residuals)

Run
  python src/18_make_report_figures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# --- Paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
IN_DIR = BASE_DIR / "reports" / "tables"
OUT_DIR = BASE_DIR / "reports" / "figures"

F_DECOMP_FULL = IN_DIR / "decomp_full.csv"
F_OPP = IN_DIR / "opportunity_list_35_39.csv"
F_HPSA_DECILES = IN_DIR / "hpsa_deciles_effect_table.csv"
F_SCALE_DECILES = IN_DIR / "scale_deciles_effect_table.csv"


def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"Missing required file: {path}")
    return pd.read_csv(path)


def savefig(name: str) -> None:
    out_path = OUT_DIR / name
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"Saved figure: {out_path}")


def year_label(df: pd.DataFrame) -> np.ndarray:
    """Convert year_2025 (0/1) into year labels used in legends."""
    if "year_2025" not in df.columns:
        return np.array(["Unknown"] * len(df))
    return np.where(df["year_2025"].astype(int) == 1, "2025", "2024")


def fig01_structural_vs_observed() -> None:
    df = read_csv(F_DECOMP_FULL)

    required = {"stars", "expected_stars_structural", "operational_residual", "year_2025"}
    missing = required - set(df.columns)
    if missing:
        die(f"decomp_full.csv missing columns: {missing}")

    df["year"] = year_label(df)
    df["is_near"] = ((df["stars"] >= 3.0) & (df["stars"] <= 3.9)).astype(int)

    x = df["expected_stars_structural"].astype(float)
    y = df["stars"].astype(float)

    plt.figure(figsize=(8.5, 6.0))

    # Plot non-near first
    m0 = df["is_near"] == 0
    plt.scatter(x[m0], y[m0], alpha=0.35, s=22, label="Other contracts")

    # Highlight near-threshold
    m1 = df["is_near"] == 1
    plt.scatter(x[m1], y[m1], alpha=0.85, s=26, label="Near-threshold (3.0–3.9)")

    lo = float(min(x.min(), y.min()))
    hi = float(max(x.max(), y.max()))
    plt.plot([lo, hi], [lo, hi], linestyle="--", linewidth=1.5)

    plt.title("Observed Stars vs Structural Expectation (Contract-Year)")
    plt.xlabel("Expected Stars (Structural Model)")
    plt.ylabel("Observed Stars")
    plt.legend(frameon=False)
    plt.grid(True, alpha=0.25)

    savefig("fig01_structural_vs_observed_scatter.png")


def fig02_residuals_by_star_band() -> None:
    df = read_csv(F_DECOMP_FULL)

    df["stars"] = df["stars"].astype(float)
    df["operational_residual"] = df["operational_residual"].astype(float)

    def band(s: float) -> str:
        if 3.0 <= s < 3.5:
            return "3.0–3.4"
        if 3.5 <= s <= 3.9:
            return "3.5–3.9"
        if s >= 4.0:
            return "4.0+"
        return "<3.0"

    df["band"] = df["stars"].apply(band)
    df = df[df["band"].isin(["3.0–3.4", "3.5–3.9", "4.0+"])].copy()

    groups = ["3.0–3.4", "3.5–3.9", "4.0+"]
    data = [df.loc[df["band"] == g, "operational_residual"].values for g in groups]

    plt.figure(figsize=(8.0, 5.3))
    plt.boxplot(data, labels=groups, showfliers=False)
    plt.axhline(0.0, linestyle="--", linewidth=1.2)
    plt.title("Operational Residuals by Star Band")
    plt.xlabel("Observed Star Band")
    plt.ylabel("Operational Residual (Observed − Structural Expectation)")
    plt.grid(True, axis="y", alpha=0.25)

    savefig("fig02_residuals_by_star_band.png")


def plot_decile_gradient(path: Path, title: str, outname: str) -> None:
    df = read_csv(path)

    required = {"year_2025", "decile", "expected_mean", "feature_mean", "n"}
    missing = required - set(df.columns)
    if missing:
        die(f"{path.name} missing columns: {missing}")

    df["year"] = year_label(df)
    df["decile"] = df["decile"].astype(int)

    plt.figure(figsize=(8.0, 5.3))
    for yr in ["2024", "2025"]:
        sub = df[df["year"] == yr].sort_values("decile")
        if sub.empty:
            continue
        plt.plot(sub["decile"], sub["expected_mean"], marker="o", linewidth=2, label=yr)

    plt.title(title)
    plt.xlabel("Decile (1 = lowest, 10 = highest)")
    plt.ylabel("Mean Expected Stars (Structural)")
    plt.xticks(range(1, int(df["decile"].max()) + 1))
    plt.legend(frameon=False)
    plt.grid(True, alpha=0.25)

    savefig(outname)


def fig03_hpsa_deciles() -> None:
    plot_decile_gradient(
        F_HPSA_DECILES,
        title="Structural Gradient: Expected Stars by HPSA Exposure Decile",
        outname="fig03_hpsa_decile_gradient.png",
    )


def fig04_scale_deciles() -> None:
    plot_decile_gradient(
        F_SCALE_DECILES,
        title="Structural Gradient: Expected Stars by Scale (Log Enrollment) Decile",
        outname="fig04_scale_decile_gradient.png",
    )


def fig05_opportunity_table_top15() -> None:
    df = read_csv(F_OPP)

    keep = [
        "contract_id",
        "stars",
        "expected_stars_structural",
        "operational_residual",
        "hpsa_exposure",
        "poverty_exposure",
        "log_enroll",
        "year_2025",
    ]
    for c in keep:
        if c not in df.columns:
            die(f"opportunity_list_35_39.csv missing required column: {c}")

    df = df[keep].head(15).copy()
    df["year"] = year_label(df)

    df_disp = df.copy()
    df_disp["stars"] = df_disp["stars"].map(lambda v: f"{float(v):.1f}")
    df_disp["expected_stars_structural"] = df_disp["expected_stars_structural"].map(lambda v: f"{float(v):.2f}")
    df_disp["operational_residual"] = df_disp["operational_residual"].map(lambda v: f"{float(v):.2f}")
    df_disp["hpsa_exposure"] = df_disp["hpsa_exposure"].map(lambda v: f"{float(v):.2f}")
    df_disp["poverty_exposure"] = df_disp["poverty_exposure"].map(lambda v: f"{float(v):.2f}")
    df_disp["log_enroll"] = df_disp["log_enroll"].map(lambda v: f"{float(v):.2f}")

    df_disp = df_disp.drop(columns=["year_2025"]).rename(columns={
        "contract_id": "Contract",
        "stars": "Observed",
        "expected_stars_structural": "Expected",
        "operational_residual": "Residual",
        "hpsa_exposure": "HPSA",
        "poverty_exposure": "Poverty",
        "log_enroll": "LogEnroll",
        "year": "Year",
    })

    plt.figure(figsize=(10.5, 4.5))
    plt.axis("off")
    plt.title("Top Opportunities (3.5–3.9): Most Negative Residuals", pad=12)

    table = plt.table(
        cellText=df_disp.values,
        colLabels=df_disp.columns,
        loc="center",
        cellLoc="center",
        colLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 1.25)

    savefig("fig05_opportunity_table_top15.png")


def main() -> None:
    print(f"Reading report tables from: {IN_DIR}")
    print(f"Saving figures to: {OUT_DIR}")
    ensure_dirs()

    for f in [F_DECOMP_FULL, F_OPP, F_HPSA_DECILES, F_SCALE_DECILES]:
        if not f.exists():
            die(f"Missing required input file: {f}")

    fig01_structural_vs_observed()
    fig02_residuals_by_star_band()
    fig03_hpsa_deciles()
    fig04_scale_deciles()
    fig05_opportunity_table_top15()

    print("\nAll figures created.")
    print(f"Open folder: {OUT_DIR}")


if __name__ == "__main__":
    main()