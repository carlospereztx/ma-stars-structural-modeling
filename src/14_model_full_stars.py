"""
14_model_full_stars.py

Models the full Medicare Advantage Star Rating distribution at the contract-year level and
computes a structural vs. operational decomposition.

What it does
- Builds a contract-year modeling frame in DuckDB
- Fits an ordinal logit model across Star categories (2.0–5.0)
- Produces:
  - expected_stars_structural: model-implied expected Stars from structural features
  - operational_residual: observed Stars minus expected structural Stars

Structural features
- poverty_exposure (SAIPE-weighted contract exposure)
- rural_exposure (RUCC-weighted contract exposure)
- hpsa_exposure (Primary Care HPSA exposure; non-designated counties treated as 0)
- geography concentration (HHI, entropy, top shares, county count)
- contract size proxy (log enrollment)

Inputs (DuckDB: db/ma_stars.duckdb)
- contract_county_weights (contract_id, year, county_fips, w_enroll, contract_year_total_enrollment)
- contract_year_stars (contract_id, contract_year, stars_rating)
- contract_year_poverty_exposure_clean (preferred) or contract_year_poverty_exposure
- contract_year_rural_exposure
- contract_year_hpsa_exposure

Outputs (DuckDB tables)
- contract_year_geo_concentration
- contract_year_enrollment
- contract_year_model_frame_fullstars
- contract_year_structural_decomp_fullstars

Notes
- The ordinal model is estimated unweighted (OrderedModel weight support is limited in this setup).
"""

from __future__ import annotations

import os
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.miscmodels.ordinal_model import OrderedModel


# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "ma_stars.duckdb")


# --- DuckDB table names ---
T_WEIGHTS = "contract_county_weights"
T_STARS = "contract_year_stars"

T_POV_PRIMARY = "contract_year_poverty_exposure_clean"
T_POV_FALLBACK = "contract_year_poverty_exposure"

T_RURAL = "contract_year_rural_exposure"
T_HPSA = "contract_year_hpsa_exposure"

# Outputs
T_GEO = "contract_year_geo_concentration"
T_ENROLL = "contract_year_enrollment"
T_FRAME = "contract_year_model_frame_fullstars"
T_DECOMP = "contract_year_structural_decomp_fullstars"


# --- Known column names ---
# weights table
W_CONTRACT = "contract_id"
W_YEAR = "year"
W_COUNTY = "county_fips"
W_WEIGHT = "w_enroll"
W_TOTAL_ENROLL = "contract_year_total_enrollment"

# stars table
S_CONTRACT = "contract_id"
S_YEAR = "contract_year"
S_STARS = "stars_rating"

# poverty table
P_CONTRACT = "contract_id"
P_POV = "poverty_exposure"

# rural table
R_CONTRACT = "contract_id"
R_RURAL = "rural_exposure"

# hpsa exposure table
H_CONTRACT = "contract_id"
H_YEAR = "contract_year"
H_EXPOSURE = "hpsa_exposure"


# --- Helpers ---
def die(msg: str) -> None:
    print(f"\nERROR: {msg}\n")
    sys.exit(1)


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return name in {r[0] for r in con.execute("SHOW TABLES").fetchall()}


def get_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return con.execute(f"DESCRIBE {table}").df()["column_name"].tolist()


def pick_year_column(cols: list[str]) -> str | None:
    """Prefer 'contract_year' if present; fall back to 'year'."""
    if "contract_year" in cols:
        return "contract_year"
    if "year" in cols:
        return "year"
    return None


# --- Main ---
def main() -> None:
    print(f"Connecting to DuckDB: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        die(f"DuckDB file not found at: {DB_PATH}")

    con = duckdb.connect(DB_PATH)

    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if not tables:
        die("No tables found in DuckDB file.")

    print("\nTables found:")
    for t in tables:
        print(f"  - {t}")

    # Required inputs
    for t in [T_WEIGHTS, T_STARS, T_RURAL, T_HPSA]:
        if t not in tables:
            die(f"Required table missing: {t}")

    # Prefer *_clean poverty table if present
    if table_exists(con, T_POV_PRIMARY):
        T_POV = T_POV_PRIMARY
    elif table_exists(con, T_POV_FALLBACK):
        T_POV = T_POV_FALLBACK
    else:
        die(f"Neither poverty table exists: {T_POV_PRIMARY} or {T_POV_FALLBACK}")

    # Detect year column variants for poverty/rural
    pov_cols = get_columns(con, T_POV)
    rural_cols = get_columns(con, T_RURAL)

    P_YEAR = pick_year_column(pov_cols)
    R_YEAR = pick_year_column(rural_cols)

    if P_YEAR is None:
        die(f"Could not find a year column in {T_POV}. Columns: {pov_cols}")
    if R_YEAR is None:
        die(f"Could not find a year column in {T_RURAL}. Columns: {rural_cols}")

    # Validate required columns exist
    for required in [P_CONTRACT, P_POV, P_YEAR]:
        if required not in pov_cols:
            die(f"Missing column {required} in {T_POV}. Columns: {pov_cols}")

    for required in [R_CONTRACT, R_RURAL, R_YEAR]:
        if required not in rural_cols:
            die(f"Missing column {required} in {T_RURAL}. Columns: {rural_cols}")

    print(f"\nUsing poverty table: {T_POV} (year column: {P_YEAR})")
    print(f"Using rural table:   {T_RURAL} (year column: {R_YEAR})")
    print(f"Using HPSA table:    {T_HPSA} ({H_EXPOSURE})")

    # --- 1) Geo concentration metrics from enrollment weights ---
    print(f"\nBuilding geo concentration from {T_WEIGHTS}.{W_WEIGHT} ...")

    geo_df = con.execute(f"""
        WITH w AS (
            SELECT
                {W_CONTRACT} AS contract_id,
                CAST({W_YEAR} AS INTEGER) AS contract_year,
                CAST({W_COUNTY} AS VARCHAR) AS county_fips,
                CAST({W_WEIGHT} AS DOUBLE) AS w
            FROM {T_WEIGHTS}
            WHERE {W_WEIGHT} IS NOT NULL AND {W_WEIGHT} > 0
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY contract_id, contract_year
                    ORDER BY w DESC
                ) AS rn
            FROM w
        )
        SELECT
            contract_id,
            contract_year,
            SUM(w*w) AS hhi,
            -SUM(w * LN(w)) AS entropy,
            MAX(w) AS top1_share,
            SUM(CASE WHEN rn <= 5 THEN w ELSE 0 END) AS top5_share,
            COUNT(*) AS n_counties
        FROM ranked
        GROUP BY contract_id, contract_year
    """).df()

    if geo_df.empty:
        die("Geo concentration build returned 0 rows.")

    con.register("geo_df", geo_df)
    con.execute(f"""
        CREATE OR REPLACE TABLE {T_GEO} AS
        SELECT * FROM geo_df
    """)
    print(f"Saved table: {T_GEO} ({len(geo_df):,} rows)")

    # --- 2) Contract-year total enrollment from weights ---
    print("\nBuilding contract_year_enrollment ...")

    enroll_df = con.execute(f"""
        SELECT
            {W_CONTRACT} AS contract_id,
            CAST({W_YEAR} AS INTEGER) AS contract_year,
            MAX(CAST({W_TOTAL_ENROLL} AS DOUBLE)) AS total_enrollment
        FROM {T_WEIGHTS}
        WHERE {W_TOTAL_ENROLL} IS NOT NULL
        GROUP BY {W_CONTRACT}, CAST({W_YEAR} AS INTEGER)
    """).df()

    if enroll_df.empty:
        die("Enrollment build returned 0 rows.")

    con.register("enroll_df", enroll_df)
    con.execute(f"""
        CREATE OR REPLACE TABLE {T_ENROLL} AS
        SELECT * FROM enroll_df
    """)
    print(f"Saved table: {T_ENROLL} ({len(enroll_df):,} rows)")

    # --- 3) Contract-year modeling frame (structural features + Stars) ---
    print(f"\nBuilding {T_FRAME} ...")

    model_df = con.execute(f"""
        WITH s AS (
            SELECT
                {S_CONTRACT} AS contract_id,
                CAST({S_YEAR} AS INTEGER) AS contract_year,
                CAST({S_STARS} AS DOUBLE) AS stars
            FROM {T_STARS}
            WHERE {S_STARS} IS NOT NULL
        ),
        p AS (
            SELECT
                {P_CONTRACT} AS contract_id,
                CAST({P_YEAR} AS INTEGER) AS contract_year,
                CAST({P_POV} AS DOUBLE) AS poverty_exposure
            FROM {T_POV}
        ),
        r AS (
            SELECT
                {R_CONTRACT} AS contract_id,
                CAST({R_YEAR} AS INTEGER) AS contract_year,
                CAST({R_RURAL} AS DOUBLE) AS rural_exposure
            FROM {T_RURAL}
        ),
        h AS (
            SELECT
                {H_CONTRACT} AS contract_id,
                CAST({H_YEAR} AS INTEGER) AS contract_year,
                CAST({H_EXPOSURE} AS DOUBLE) AS hpsa_exposure
            FROM {T_HPSA}
        ),
        g AS (
            SELECT
                contract_id,
                CAST(contract_year AS INTEGER) AS contract_year,
                CAST(hhi AS DOUBLE) AS hhi,
                CAST(entropy AS DOUBLE) AS entropy,
                CAST(top1_share AS DOUBLE) AS top1_share,
                CAST(top5_share AS DOUBLE) AS top5_share,
                CAST(n_counties AS DOUBLE) AS n_counties
            FROM {T_GEO}
        ),
        e AS (
            SELECT
                contract_id,
                CAST(contract_year AS INTEGER) AS contract_year,
                CAST(total_enrollment AS DOUBLE) AS total_enrollment
            FROM {T_ENROLL}
        )
        SELECT
            s.contract_id,
            s.contract_year,
            s.stars,
            p.poverty_exposure,
            r.rural_exposure,
            h.hpsa_exposure,
            g.hhi,
            g.entropy,
            g.top1_share,
            g.top5_share,
            g.n_counties,
            e.total_enrollment
        FROM s
        LEFT JOIN p
            ON s.contract_id = p.contract_id
           AND s.contract_year = p.contract_year
        LEFT JOIN r
            ON s.contract_id = r.contract_id
           AND s.contract_year = r.contract_year
        LEFT JOIN h
            ON s.contract_id = h.contract_id
           AND s.contract_year = h.contract_year
        LEFT JOIN g
            ON s.contract_id = g.contract_id
           AND s.contract_year = g.contract_year
        LEFT JOIN e
            ON s.contract_id = e.contract_id
           AND s.contract_year = e.contract_year
    """).df()

    if model_df.empty:
        die("Model frame returned 0 rows. Check joins and upstream tables.")

    con.register("model_df", model_df)
    con.execute(f"""
        CREATE OR REPLACE TABLE {T_FRAME} AS
        SELECT * FROM model_df
    """)
    print(f"Saved table: {T_FRAME} ({len(model_df):,} rows)")

    # --- 4) Ordinal logit model over Stars distribution ---
    df = model_df.copy()
    df = df.rename(columns={"contract_year": "year"})

    core = [
        "stars",
        "poverty_exposure",
        "rural_exposure",
        "hpsa_exposure",
        "hhi",
        "entropy",
        "top1_share",
        "top5_share",
        "n_counties",
    ]

    df = df.dropna(subset=core).copy()
    if df.empty:
        die("After dropping missing core fields, 0 rows remain.")

    df["total_enrollment"] = df["total_enrollment"].fillna(1).clip(lower=1)
    df["log_enroll"] = np.log(df["total_enrollment"])

    levels = np.sort(df["stars"].unique())
    if len(levels) < 3:
        die(f"Not enough distinct star levels. Found: {levels}")

    df["stars_ord"] = df["stars"].map({v: i for i, v in enumerate(levels)}).astype(int)

    # Year fixed effects (baseline year dropped)
    df = pd.get_dummies(df, columns=["year"], drop_first=True)
    for c in df.columns:
        if c.startswith("year_"):
            df[c] = df[c].astype(int)

    X_cols = [
        "poverty_exposure",
        "rural_exposure",
        "hpsa_exposure",
        "hhi",
        "entropy",
        "top1_share",
        "top5_share",
        "n_counties",
        "log_enroll",
    ] + [c for c in df.columns if c.startswith("year_")]

    for c in X_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=X_cols + ["stars_ord"]).copy()

    X = df[X_cols]
    y = df["stars_ord"]

    print("\n==============================")
    print("Ordered Logit (unweighted)")
    print("==============================")
    ord_mod = OrderedModel(y, X, distr="logit")
    ord_res = ord_mod.fit(method="bfgs", disp=False)
    print(ord_res.summary())

    print("\n==============================")
    print("Multinomial Logit (robustness check)")
    print("==============================")
    try:
        mn_mod = sm.MNLogit(y, sm.add_constant(X))
        mn_res = mn_mod.fit(method="newton", disp=False, maxiter=200)
        print(mn_res.summary())
    except Exception as e:
        print("NOTE: MNLogit failed (often due to sparse categories or separation).")
        print(f"Exception: {e}")

    # --- 5) Structural decomposition ---
    print("\n==============================")
    print("Structural decomposition")
    print("==============================")

    # Predicted category probabilities (n x K), expected Stars = sum_k P_k * level_k
    probs = ord_res.model.predict(ord_res.params, exog=X)
    expected_stars = np.asarray(probs) @ levels

    df["expected_stars_structural"] = expected_stars
    df["operational_residual"] = df["stars"] - df["expected_stars_structural"]

    out_cols = [
        "contract_id",
        "stars",
        "expected_stars_structural",
        "operational_residual",
        "poverty_exposure",
        "rural_exposure",
        "hpsa_exposure",
        "hhi",
        "entropy",
        "top1_share",
        "top5_share",
        "n_counties",
        "total_enrollment",
        "log_enroll",
    ] + [c for c in df.columns if c.startswith("year_")]

    out = df[out_cols].copy()

    con.register("decomp_df", out)
    con.execute(f"""
        CREATE OR REPLACE TABLE {T_DECOMP} AS
        SELECT * FROM decomp_df
    """)
    print(f"Saved table: {T_DECOMP} ({len(out):,} rows)")

    # --- 6) Sanity checks ---
    print("\n==============================")
    print("Sanity checks")
    print("==============================")
    print("Star levels:", levels)
    print("Rows modeled:", len(df))
    print("\nOperational residual summary:")
    print(out["operational_residual"].describe())

    print("\nDone.")


if __name__ == "__main__":
    main()