
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
    g.to_csv(f"{args.outdir}/ret_globals.csv", index=False)
    inflow.to_csv(f"{args.outdir}/ret_inflows.csv", index=False)
    outflow.to_csv(f"{args.outdir}/ret_outflows.csv", index=False)

    # NEW: write balances now (using the query you already have up top)
    starting_balances_df.to_csv(f"{args.outdir}/ret_starting_balances.csv", index=False)

    print("Wrote:",
          f"{args.outdir}/ret_globals.csv",
          f"{args.outdir}/ret_inflows.csv",
          f"{args.outdir}/ret_outflows.csv",
          f"{args.outdir}/ret_starting_balances.csv")

