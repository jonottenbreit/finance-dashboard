
import pandas as pd
import numpy as np
import os
from pathlib import Path
import duckdb
from typing import Tuple, Dict, Any
from datetime import date

# Create connections

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH  = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))

con = duckdb.connect(str(DB_PATH))

sql_balances = """
WITH ranked AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (
      PARTITION BY p.account_id, p.symbol
      ORDER BY p.as_of_date DESC
    ) AS rn
  FROM positions_enriched p
)
SELECT
  p.account_id,
  COALESCE(a.account_name, p.account_id) AS account_name,
  COALESCE(a.Tax_Bucket, 'Unknown')      AS tax_bucket_final,
  COALESCE(a.owner, '')                  AS owner,
  SUM(p.value)                           AS balance_today
FROM ranked p
LEFT JOIN account_dim a
  ON a.account_id = p.account_id
WHERE p.rn = 1
  AND COALESCE(a.include_networth, TRUE)
GROUP BY
  p.account_id,
  account_name,
  COALESCE(a.Tax_Bucket, 'Unknown'),
  COALESCE(a.owner, '')
ORDER BY p.account_id;
"""

starting_balances_df = con.execute(sql_balances).df()
# optional: write for debugging / PBI import
# starting_balances_df.to_csv(<outdir>/ret_starting_balances.csv, index=False)

RATE_VAR_HINTS = {
    "inflation_rate",
    "real_return_rate",
    "cola_rate",
    "employee_pct_of_salary",
    "employer_match_rate",
    "tax_rate_working",
    "tax_rate_retirement"
}

def ensure_v_dividend_flows(con: duckdb.DuckDBPyConnection) -> None:
    """
    Creates/refreshes a view that summarizes annual dividend flows by account,
    using security_dim.dividend_yield and security_dim.qualified_ratio.

    It reads tax_rate_working / tax_rate_retirement from TEMP table retirement_globals
    (registered from the parsed CSV in __main__).
    """
    con.execute("""
    CREATE OR REPLACE VIEW v_dividend_flows AS
    WITH latest AS (
      SELECT
        p.*,
        ROW_NUMBER() OVER (
          PARTITION BY p.account_id, p.symbol
          ORDER BY p.as_of_date DESC
        ) AS rn
      FROM positions_enriched p
    ),
    j AS (
      SELECT
        a.account_id,
        COALESCE(a.account_name, l.account_id)    AS account_name,
        COALESCE(a.acct_group, 'Unknown')         AS acct_group,
        COALESCE(a.tax_bucket, 'Unknown')         AS tax_bucket,
        l.symbol,
        s.dividend_yield,
        COALESCE(s.qualified_ratio, 0.0)          AS qualified_ratio,
        SUM(l.value)                              AS value
      FROM latest l
      LEFT JOIN security_dim s ON s.symbol = l.symbol
      LEFT JOIN account_dim a  ON a.account_id = l.account_id
      WHERE l.rn = 1
        AND COALESCE(a.include_networth, TRUE)
        AND s.dividend_yield IS NOT NULL
      GROUP BY 1,2,3,4,5,6,7
    ),
    rates AS (
      -- Pull working/retirement rates from the TEMP table we register in __main__
      SELECT
        COALESCE(MAX(CASE WHEN lower(Variable)='tax_rate_working'    THEN CAST(Value AS DOUBLE) END), 0.30) AS tax_rate_working,
        COALESCE(MAX(CASE WHEN lower(Variable)='tax_rate_retirement' THEN CAST(Value AS DOUBLE) END), 0.25) AS tax_rate_retirement,
        0.15 AS qualified_div_rate
      FROM retirement_globals
    )
    SELECT
      j.account_id,
      j.account_name,
      j.acct_group,
      j.tax_bucket,
      SUM(j.value * j.dividend_yield) AS dividends_gross,
      SUM(j.value * j.dividend_yield
          * (1 - ((1 - j.qualified_ratio) * r.tax_rate_working
                  + j.qualified_ratio     * r.qualified_div_rate))) AS dividends_net_working,
      SUM(j.value * j.dividend_yield
          * (1 - ((1 - j.qualified_ratio) * r.tax_rate_retirement
                  + j.qualified_ratio     * r.qualified_div_rate))) AS dividends_net_retirement
    FROM j
    CROSS JOIN rates r
    GROUP BY 1,2,3,4
    ORDER BY account_name;
    """)

def ensure_v_dividend_flows_by_year(con):
    """
    Project dividends by YEAR with contributions compounding account values.
    Uses nominal growth g = (1+real_return_rate)*(1+inflation_rate)-1.
    - Uses ret_inflows (already filtered to contributions in Python) as the source of investable cash.
    - Maps contributions by account_id if present; else by name via applies_to or Variable.
    """
    con.execute("""
    CREATE OR REPLACE VIEW v_dividend_flows_by_year AS
    WITH RECURSIVE
    -- 1) Params (cast first, then coalesce)
    raw AS (
      SELECT
        /* globals pulled from Variable/Value rows */
        TRY_CAST(MAX(CASE WHEN lower(Variable)='current_year'        THEN Value END) AS INTEGER) AS current_year_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='end_year'            THEN Value END) AS INTEGER) AS end_year_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='retirement_year'     THEN Value END) AS INTEGER) AS retirement_year_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='death_year'          THEN Value END) AS INTEGER) AS death_year_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='real_return_rate'    THEN Value END) AS DOUBLE)  AS real_r_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='inflation_rate'      THEN Value END) AS DOUBLE)  AS infl_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='tax_rate_working'    THEN Value END) AS DOUBLE)  AS tax_work_raw,
        TRY_CAST(MAX(CASE WHEN lower(Variable)='tax_rate_retirement' THEN Value END) AS DOUBLE)  AS tax_ret_raw,

        /* horizon signals coming from ANY timed rows in the CSV */
        MIN(TRY_CAST(Start_Year AS INTEGER)) AS start_year_from_rows,
        MAX(
          CASE
            WHEN TRY_CAST(Start_Year AS INTEGER) IS NOT NULL
            AND TRY_CAST(Duration   AS INTEGER) IS NOT NULL
            THEN TRY_CAST(Start_Year AS INTEGER) + TRY_CAST(Duration AS INTEGER) - 1
            ELSE NULL
          END
        ) AS max_end_year_from_numeric,
        /* flag if any row uses 'lifetime' / 'life' / 'perpetuity' */
        MAX(
          CASE
            WHEN lower(COALESCE(Duration,'')) IN ('lifetime','life','perpetuity','forever') THEN 1
            ELSE 0
          END
        ) AS has_lifetime
      FROM retirement_globals
    ),

    params AS (
      SELECT
        /* start_year priority: any timed row’s earliest Start_Year -> current_year -> this year */
        CAST(
          COALESCE(
            start_year_from_rows,
            current_year_raw,
            CAST(strftime(now(), '%Y') AS INTEGER)
          ) AS INTEGER
        ) AS start_year,

        /* retirement year default */
        CAST(
          COALESCE(
            retirement_year_raw,
            CAST(strftime(now(), '%Y') AS INTEGER) + 15
          ) AS INTEGER
        ) AS retirement_year,

        /* nominal growth */
        CAST(COALESCE(real_r_raw, 0.0) AS DOUBLE) AS real_r,
        CAST(COALESCE(infl_raw,  0.0) AS DOUBLE) AS infl,
        CAST(COALESCE(tax_work_raw, 0.30) AS DOUBLE) AS tax_work,
        CAST(COALESCE(tax_ret_raw,  0.25) AS DOUBLE) AS tax_ret,
        ( (1.0 + CAST(COALESCE(real_r_raw, 0.0) AS DOUBLE))
          * (1.0 + CAST(COALESCE(infl_raw,  0.0) AS DOUBLE)) - 1.0 ) AS g,

        /* end_year priority:
          1) explicit end_year
          2) any 'lifetime' row -> death_year (or 2100)
          3) numeric Start_Year + Duration from rows
          4) fallback: start_year + 40
        */
        CAST(
          COALESCE(
            end_year_raw,
            CASE WHEN has_lifetime = 1 THEN COALESCE(death_year_raw, 2100) END,
            max_end_year_from_numeric,
            /* fallback uses the start_year we just computed */
            (COALESCE(start_year_from_rows, current_year_raw, CAST(strftime(now(), '%Y') AS INTEGER)) + 40)
          ) AS INTEGER
        ) AS end_year
      FROM raw
    )
,

    -- 2) Latest positions & account-level weighted yield/qualified mix (today)
    latest AS (
      SELECT p.*,
             ROW_NUMBER() OVER (PARTITION BY p.account_id, p.symbol ORDER BY p.as_of_date DESC) rn
      FROM positions_enriched p
    ),
    acct_holdings AS (
      SELECT
        a.account_id,
        a.account_name,
        a.acct_group,
        a.tax_bucket,
        l.symbol,
        SUM(l.value) AS value
      FROM latest l
      JOIN account_dim a ON a.account_id = l.account_id
      WHERE l.rn = 1
        AND COALESCE(a.include_networth, TRUE)
      GROUP BY 1,2,3,4,5
    ),
    acct_yield AS (
      SELECT
        h.account_id,
        MAX(h.account_name) AS account_name,
        MAX(h.acct_group)   AS acct_group,
        MAX(h.tax_bucket)   AS tax_bucket,
        SUM(h.value * COALESCE(s.dividend_yield,0.0)) / NULLIF(SUM(h.value),0)  AS w_div_yield,
        SUM(h.value * COALESCE(s.qualified_ratio,0.0)) / NULLIF(SUM(h.value),0) AS w_q_ratio
      FROM acct_holdings h
      LEFT JOIN security_dim s ON s.symbol = h.symbol
      GROUP BY 1
    ),

    -- 3) Investable contributions mapped to accounts (yearly)
    --    We’ll accept ANY of: account_id, applies_to, or Variable as the account key.
    infl_norm AS (
      SELECT
        TRY_CAST(Year AS INTEGER)  AS year,
        TRY_CAST(Value AS DOUBLE)  AS amount,

        /* Pick up all potential account keys from the inflow row */
        NULLIF(TRIM(account_id), '')   AS account_id_text,
        NULLIF(TRIM(applies_to), '')   AS applies_to_text,
        NULLIF(TRIM(Variable),   '')   AS variable_text
      FROM ret_inflows
    ),

    investable_contribs AS (
      SELECT
        /* prefer explicit account_id; else match Variable/applies_to to either account_id OR account_name */
        COALESCE(a_id.account_id, a_var_id.account_id, a_app_id.account_id,
                a_var_name.account_id, a_app_name.account_id) AS account_id,
        n.year,
        SUM(COALESCE(n.amount,0.0)) AS amount
      FROM infl_norm n
      /* 1) exact account_id from column */
      LEFT JOIN account_dim a_id
        ON a_id.account_id = n.account_id_text

      /* 2) Variable matches account_id */
      LEFT JOIN account_dim a_var_id
        ON a_var_id.account_id = n.variable_text

      /* 3) applies_to matches account_id */
      LEFT JOIN account_dim a_app_id
        ON a_app_id.account_id = n.applies_to_text

      /* 4) Variable matches account_name (case-insensitive) */
      LEFT JOIN account_dim a_var_name
        ON LOWER(a_var_name.account_name) = LOWER(n.variable_text)

      /* 5) applies_to matches account_name (case-insensitive) */
      LEFT JOIN account_dim a_app_name
        ON LOWER(a_app_name.account_name) = LOWER(n.applies_to_text)

      WHERE n.year IS NOT NULL
        AND n.amount IS NOT NULL
        AND n.amount <> 0
        AND COALESCE(a_id.account_id, a_var_id.account_id, a_app_id.account_id,
                    a_var_name.account_id, a_app_name.account_id) IS NOT NULL
      GROUP BY 1,2
    )
    ,

    -- 4) Starting values per account (today)
    start_values AS (
      SELECT account_id, SUM(value) AS start_value
      FROM acct_holdings
      GROUP BY 1
    ),

    -- 5) Year rows (materialize, not list)
    years AS (
      SELECT y AS year
      FROM params p, range(p.start_year, p.end_year + 1) AS t(y)
    ),

    -- 6) Recursive roll-forward per account:
    --    V[t] = (V[t-1] + contrib[t]) * (1 + g)
    pv(account_id, year, value) AS (
      SELECT s.account_id, p.start_year AS year, COALESCE(s.start_value, 0.0) AS value
      FROM start_values s, params p
      UNION ALL
      SELECT pv.account_id,
             pv.year + 1,
             ( pv.value + COALESCE(c.amount, 0.0) ) * (1.0 + p.g)
      FROM pv
      JOIN params p ON pv.year < p.end_year
      LEFT JOIN investable_contribs c
        ON c.account_id = pv.account_id AND c.year = pv.year + 1
    ),

    -- 7) Compose per-year dividend math off projected values
    divs AS (
      SELECT
        y.year,
        a.account_id,
        a.account_name,
        a.acct_group,
        a.tax_bucket,
        pv.value                 AS portfolio_value,
        a.w_div_yield            AS div_yield_w,
        a.w_q_ratio              AS q_ratio,
        CASE WHEN y.year < p.retirement_year THEN p.tax_work ELSE p.tax_ret END AS eff_ord_rate,
        0.15 AS qd_rate
      FROM years y
      CROSS JOIN params p
      JOIN pv         ON pv.year = y.year
      JOIN acct_yield a ON a.account_id = pv.account_id
    )

    SELECT
      d.year,
      d.account_id,
      d.account_name,
      d.acct_group,
      d.tax_bucket,
      (d.portfolio_value * COALESCE(d.div_yield_w,0.0)) AS dividends_gross,
      (d.portfolio_value * COALESCE(d.div_yield_w,0.0)
        * (1.0 - ( (1.0 - COALESCE(d.q_ratio,0.0)) * d.eff_ord_rate
                   + COALESCE(d.q_ratio,0.0) * d.qd_rate ))) AS dividends_net_by_year,
      (d.portfolio_value * COALESCE(d.div_yield_w,0.0)
        * (1.0 - ( (1.0 - COALESCE(d.q_ratio,0.0)) * (SELECT tax_work FROM params)
                   + COALESCE(d.q_ratio,0.0) * 0.15 ))) AS dividends_net_working,
      (d.portfolio_value * COALESCE(d.div_yield_w,0.0)
        * (1.0 - ( (1.0 - COALESCE(d.q_ratio,0.0)) * (SELECT tax_ret FROM params)
                   + COALESCE(d.q_ratio,0.0) * 0.15 ))) AS dividends_net_retirement
    FROM divs d
    ORDER BY d.year, d.account_name;
    """)

def looks_like_rate(var: str) -> bool:
    var_lower = (var or "").lower()
    return (
        var_lower in RATE_VAR_HINTS
        or var_lower.endswith("_rate")
        or var_lower.endswith("_pct")
        or var_lower.endswith("_percentage")
    )

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    for col in ["Category","Variable","Value","Start_Year","Duration"]:
        if col not in df.columns:
            df[col] = np.nan
    df = df.replace({"": np.nan, "NA": np.nan, "None": np.nan})
    return df

def parse_value(var: str, val) -> Tuple[float, bool]:
    if pd.isna(val):
        v = np.nan
    else:
        v = float(val)
    return v, looks_like_rate(var)

def resolve_end_year(start_year, duration, globals_map: Dict[str, Any]) -> int:
    if pd.isna(start_year):
        return np.nan
    start_year = int(start_year)
    if pd.isna(duration):
        return start_year
    dur_str = str(duration).strip().lower()
    if dur_str == "lifetime":
        death_year = int(globals_map.get("death_year"))
        return death_year
    years = int(dur_str)
    return start_year + years - 1

def inflate_series(amount_today: float, start_year: int, end_year: int, inflation_rate: float):
    years = list(range(start_year, end_year + 1))
    idx = np.arange(len(years))
    series = pd.Series(amount_today * np.power(1.0 + inflation_rate, idx), index=years, dtype=float)
    return series

def load_retirement_assumptions(path: str):
    
    df = pd.read_csv(path)
    df = sanitize_columns(df)

    is_time_bound = df["Start_Year"].notna() | df["Duration"].notna()
    globals_rows = df.loc[~is_time_bound].copy()
    timed_rows = df.loc[is_time_bound].copy()

    globals_map = {}
    for _, r in globals_rows.iterrows():
        var = str(r.get("Variable"))
        val, is_rate = parse_value(var, r.get("Value"))
        globals_map[var] = val

    for must in ["inflation_rate","real_return_rate","death_year"]:
        if must not in globals_map or pd.isna(globals_map[must]):
            raise ValueError(f"Missing required global assumption '{must}' in {path}")

    # define base_year + inflation_rate AFTER globals_map exists
    base_year = int(globals_map.get("current_year", date.today().year))
    inflation_rate = float(globals_map["inflation_rate"])

    inflows_records = []
    outflows_records = []

    for _, r in timed_rows.iterrows():
        cat = (r.get("Category") or "").strip().lower()
        var = str(r.get("Variable"))
        val, is_rate = parse_value(var, r.get("Value"))
        start_year = r.get("Start_Year")
        end_year = resolve_end_year(start_year, r.get("Duration"), globals_map)

        if pd.isna(start_year) or pd.isna(end_year):
            globals_map[var] = val
            continue

        start_year = int(start_year); end_year = int(end_year)

        name_lower = var.lower()
        is_social_security = ("socialsecurity" in name_lower)
        is_monthly = ("_monthly" in name_lower)

        # ... inflation decision:
        inflate = True if (cat == "withdrawal" or is_social_security) else False

        # convert monthly → annual if you used *_monthly naming
        if not is_rate and is_monthly:
            val = val * 12.0

        if is_rate:
            years = list(range(start_year, end_year + 1))
            series = pd.Series([val] * len(years), index=years, dtype=float)
        else:
            if inflate:
                # PRE-INFLATE to start year, then continue inflating each year
                years = list(range(start_year, end_year + 1))
                # bring today's-dollars 'val' to nominal at start_year
                start_nominal = float(val) * ((1.0 + inflation_rate) ** max(0, (start_year - base_year)))
                idx = np.arange(len(years))
                series = pd.Series(start_nominal * np.power(1.0 + inflation_rate, idx), index=years, dtype=float)
            else:
                years = list(range(start_year, end_year + 1))
                series = pd.Series([val] * len(years), index=years, dtype=float)

        for year, amt in series.items():
            rec = {
                "Year": int(year),
                "Variable": var,
                "Value": float(amt),
            }
            for opt in ["account_id","applies_to","Notes"]:
                if opt in r and pd.notna(r[opt]):
                    rec[opt] = r[opt]
            if cat == "contribution":
                inflows_records.append(rec)
            elif cat == "withdrawal":
                outflows_records.append(rec)

    inflows_df = pd.DataFrame(inflows_records, columns=["Year","Variable","Value","account_id","applies_to","Notes"]).fillna("")
    outflows_df = pd.DataFrame(outflows_records, columns=["Year","Variable","Value","account_id","applies_to","Notes"]).fillna("")
    globals_df = pd.DataFrame(
        [{"Variable": k, "Value": v} for k, v in globals_map.items()],
        columns=["Variable","Value"]
    )

    return globals_df, inflows_df, outflows_df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load retirement assumptions CSV and emit normalized tables.")
    parser.add_argument("--csv", required=True, help="Path to retirement_assumptions.csv")
    parser.add_argument("--outdir", default=".", help="Output directory for globals/inflows/outflows CSVs")
    args = parser.parse_args()

    g, inflow, outflow = load_retirement_assumptions(args.csv)
    con.register("ret_inflows", inflow)
    g.to_csv(f"{args.outdir}/ret_globals.csv", index=False)
    inflow.to_csv(f"{args.outdir}/ret_inflows.csv", index=False)
    outflow.to_csv(f"{args.outdir}/ret_outflows.csv", index=False)

    # New: expose globals inside DuckDB, then create the dividends view
    con.execute("CREATE OR REPLACE TEMP TABLE retirement_globals AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(args.csv)])
    # (alternative) con.register("retirement_globals", g)
    ensure_v_dividend_flows(con)

    # Build the by-year projection view (dividends grow with nominal portfolio rate)
    ensure_v_dividend_flows_by_year(con)

    # Optional export for Power BI
    div_by_year = con.execute("""
        SELECT year, account_name, acct_group, tax_bucket, dividends_net_by_year
        FROM v_dividend_flows_by_year
        ORDER BY year, account_name
    """).fetchdf()
    div_by_year.to_csv(f"{args.outdir}/ret_dividend_flows_by_year.csv", index=False)

    # Optional: materialize a CSV Power BI can import directly
    div_preview = con.execute("""
        SELECT account_name, acct_group, tax_bucket,
               dividends_gross, dividends_net_working, dividends_net_retirement
        FROM v_dividend_flows
        ORDER BY account_name
    """).fetchdf()
    div_preview.to_csv(f"{args.outdir}/ret_dividend_flows.csv", index=False)

    # existing: balances
    starting_balances_df.to_csv(f"{args.outdir}/ret_starting_balances.csv", index=False)

    print("Wrote:",
          f"{args.outdir}/ret_globals.csv",
          f"{args.outdir}/ret_inflows.csv",
          f"{args.outdir}/ret_outflows.csv",
          f"{args.outdir}/ret_starting_balances.csv",
          f"{args.outdir}/ret_dividend_flows.csv")


