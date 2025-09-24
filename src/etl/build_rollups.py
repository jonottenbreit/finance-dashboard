"""
Build reporting rollups and export Parquet snapshots.
Run:
    python src/etl/build_rollups.py
"""
# src/etl/build_rollups.py
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

# -------------------------
# ENV / PATHS
# -------------------------
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_ENV = os.getenv("DUCKDB_PATH")
DB_PATH = Path(DB_ENV) if DB_ENV else (DATA_DIR / "finance.duckdb")
EXPORTS_DIR = DATA_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))

# -------------------------
# HELPERS
# -------------------------
def has_table(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        return con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1", [name]).fetchone() is not None
    except Exception:
        return False

def has_column(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    try:
        return con.execute("SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ? LIMIT 1", [table, col]).fetchone() is not None
    except Exception:
        return False

def safe_copy(table: str, filename: str):
    if has_table(con, table):
        dst = (EXPORTS_DIR / filename).as_posix()
        con.execute(f"COPY {table} TO '{dst}' (FORMAT PARQUET)")
        print(f"Exported {table} -> {dst}")
    else:
        print(f"SKIP export {table}: table not found")

# -------------
# Ensures rules are loaded
# -------------

# --- ensure rule tables exist from CSVs if missing ---
rules_dir = (Path(os.getenv("RULES_DIR")) if os.getenv("RULES_DIR")
             else Path(__file__).resolve().parents[2] / "rules")

# security_dim (create if missing, else add new cols if needed)
if not has_table(con, "security_dim"):
    sec_csv = rules_dir / "security_dim.csv"
    if sec_csv.exists():
        con.execute("""
            CREATE TABLE security_dim (
              symbol TEXT PRIMARY KEY,
              asset_class TEXT,
              region TEXT,
              style TEXT,
              size TEXT,
              expense_ratio DECIMAL(9,6)
            )
        """)
        con.execute("INSERT INTO security_dim SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(sec_csv)])
        print("Loaded security_dim from CSV")
else:
    # add columns if the table pre-existed without them
    if not has_column(con, "security_dim", "size"):
        con.execute("ALTER TABLE security_dim ADD COLUMN size TEXT;")
    if not has_column(con, "security_dim", "expense_ratio"):
        con.execute("ALTER TABLE security_dim ADD COLUMN expense_ratio DECIMAL(9,6);")

if not has_table(con, "target_allocation"):
    targ_csv = rules_dir / "target_allocation.csv"
    if targ_csv.exists():
        con.execute("""
            CREATE TABLE target_allocation (
              asset_class TEXT PRIMARY KEY,
              target_weight DOUBLE
            )
        """)
        con.execute("INSERT INTO target_allocation SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(targ_csv)])
        print("Loaded target_allocation from CSV")

# -------------------------
# MONTH DIM (union of sources present)
# -------------------------
unions = []

if has_table(con, "transactions") and has_column(con, "transactions", "date"):
    unions.append("SELECT date_trunc('month', date)::DATE AS m FROM transactions")

if has_table(con, "budget_monthly") and has_column(con, "budget_monthly", "month"):
    unions.append("SELECT date_trunc('month', month)::DATE AS m FROM budget_monthly")

# balances
if has_table(con, "balance_snapshot") and has_column(con, "balance_snapshot", "as_of_date"):
    unions.append("SELECT date_trunc('month', as_of_date)::DATE AS m FROM balance_snapshot")

# positions
if has_table(con, "positions") and has_column(con, "positions", "as_of_date"):
    unions.append("SELECT date_trunc('month', as_of_date)::DATE AS m FROM positions")

if unions:
    con.execute(f"""
        CREATE OR REPLACE TABLE month_dim AS
        SELECT DISTINCT m::DATE AS month
        FROM (
            {' UNION ALL '.join(unions)}
        )
        ORDER BY 1;
    """)
    print("Built month_dim")
else:
    print("SKIP month_dim: no date-bearing sources found")

# -------------------------
# CASHFLOW ROLLUP
# -------------------------
if has_table(con, "transactions") and has_column(con, "transactions", "amount_cents") and has_column(con, "transactions", "date"):
    # exclude transfers if column available
    where_clause = "WHERE COALESCE(is_transfer, FALSE) = FALSE" if has_column(con, "transactions", "is_transfer") else ""

    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_cashflow AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', date) AS month,
            CAST(amount_cents/100.0 AS DOUBLE) AS amt
          FROM transactions
          {where_clause}
        )
        SELECT
          month,
          SUM(CASE WHEN amt > 0 THEN amt ELSE 0 END) AS income,
          SUM(CASE WHEN amt < 0 THEN amt ELSE 0 END) AS spending,  -- negative
          SUM(amt) AS net_cashflow
        FROM t
        GROUP BY 1
        ORDER BY 1;
    """)
    print("Built monthly_cashflow")
else:
    print("SKIP monthly_cashflow: transactions table/columns not found")

# -------------------------
# ACTUALS BY CATEGORY (P&L detail)
# -------------------------
if has_table(con, "transactions") and has_column(con, "transactions", "category") and has_column(con, "transactions", "date") and has_column(con, "transactions", "amount_cents"):
    where_clause = "WHERE COALESCE(is_transfer, FALSE) = FALSE" if has_column(con, "transactions", "is_transfer") else ""
    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_actuals_by_category AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', date) AS month,
            category,
            CAST(amount_cents/100.0 AS DOUBLE) AS amt
          FROM transactions
          {where_clause}
        )
        SELECT
          month,
          category,
          SUM(amt)                         AS actual_signed,         -- keep signs
          SUM(CASE WHEN amt < 0 THEN -amt ELSE 0 END) AS spending   -- positive spend
        FROM t
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    print("Built monthly_actuals_by_category")
else:
    print("SKIP monthly_actuals_by_category: category or needed columns missing")

# -------------------------
# NET WORTH ROLLUPS
# -------------------------
if has_table(con, "balance_snapshot") and has_table(con, "account_dim"):
    # Normalize liabilities to negative per account_dim.type
    # (If balances already normalized earlier, this is still safe.)
    con.execute("""
        CREATE OR REPLACE VIEW balances_enriched AS
        SELECT
          date_trunc('month', b.as_of_date)::DATE AS month_date,
          strftime('%Y-%m', b.as_of_date)        AS month,
          b.account_id,
          a.account_name,
          a.type,
          a.acct_group,
          a.tax_bucket,
          a.liquidity,
          a.include_networth,
          a.include_liquid,
          CASE WHEN LOWER(COALESCE(a.type,'')) = 'liability'
               THEN -1.0 * CAST(b.balance AS DOUBLE)
               ELSE CAST(b.balance AS DOUBLE)
          END AS balance_norm
        FROM balance_snapshot b
        LEFT JOIN account_dim a USING(account_id);
    """)
    # Monthly totals
    con.execute("""
        CREATE OR REPLACE TABLE monthly_net_worth AS
        SELECT
          month,
          SUM(CASE WHEN include_networth THEN balance_norm ELSE 0 END)                           AS net_worth,
          SUM(CASE WHEN include_networth AND balance_norm > 0 THEN balance_norm ELSE 0 END)     AS assets,
          SUM(CASE WHEN include_networth AND balance_norm < 0 THEN balance_norm ELSE 0 END)     AS liabilities,
          SUM(CASE WHEN include_liquid    THEN balance_norm ELSE 0 END)                          AS liquid_net_worth,
          SUM(CASE WHEN include_networth AND liquidity IN ('investable') THEN balance_norm ELSE 0 END) AS investable_assets
        FROM balances_enriched
        GROUP BY 1
        ORDER BY 1;
    """)
    # By group (EOP-friendly)
    con.execute("""
        CREATE OR REPLACE TABLE monthly_net_worth_by_group AS
        SELECT
          month,
          acct_group,
          SUM(CASE WHEN include_networth THEN balance_norm ELSE 0 END) AS value
        FROM balances_enriched
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    print("Built monthly_net_worth & monthly_net_worth_by_group")
else:
    print("SKIP net worth: balance_snapshot or account_dim missing")

# -------------------------
# ALLOCATION (positions + security_dim + account_dim)
# -------------------------
if has_table(con, "positions"):
    join_sec = "LEFT JOIN security_dim s USING(symbol)" if has_table(con, "security_dim") else "LEFT JOIN (SELECT NULL) s ON FALSE"
    join_acct = "LEFT JOIN account_dim a USING(account_id)" if has_table(con, "account_dim") else "LEFT JOIN (SELECT NULL) a ON FALSE"

    con.execute(f"""
        CREATE OR REPLACE VIEW positions_enriched AS
        SELECT
        p.as_of_date,
        strftime('%Y-%m', p.as_of_date) AS month,
        p.account_id,
        COALESCE(a.acct_group, 'Unknown')   AS acct_group,
        COALESCE(a.tax_bucket, 'Unknown')   AS tax_bucket,
        COALESCE(a.liquidity, 'Unknown')    AS liquidity,
        p.symbol,
        COALESCE(s.asset_class, 'Unknown')  AS asset_class,
        COALESCE(s.region, 'Unknown')       AS region,
        COALESCE(s.style, 'Unknown')        AS style,
        COALESCE(s.size, 'Unknown')         AS size,
        COALESCE(s.expense_ratio, 0.0)      AS expense_ratio,
        CAST(p.market_value AS DOUBLE)      AS value
        FROM positions p
        {join_sec}
        {join_acct};
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_allocation AS
        SELECT
          month,
          asset_class,
          region,
          style,
          tax_bucket,
          SUM(value) AS value
        FROM positions_enriched
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_allocation_by_size AS
        SELECT
        month,
        asset_class,
        region,
        style,
        size,
        tax_bucket,
        SUM(value) AS value
        FROM positions_enriched
        GROUP BY 1,2,3,4,5,6
        ORDER BY 1,2,5;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_weighted_expense_ratio AS
        SELECT
        month,
        asset_class,
        SUM(value * expense_ratio) / NULLIF(SUM(value), 0) AS weighted_expense_ratio
        FROM positions_enriched
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW investable_by_month AS
        SELECT month, SUM(value) AS investable_value
        FROM positions_enriched
        GROUP BY month;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW allocation_weights AS
        SELECT
          m.month,
          m.asset_class,
          SUM(m.value) AS value,
          i.investable_value,
          SUM(m.value) / NULLIF(i.investable_value, 0) AS actual_weight
        FROM monthly_allocation m
        JOIN investable_by_month i USING(month)
        GROUP BY 1,2,4;
    """)

    if has_table(con, "target_allocation"):
        con.execute("""
            CREATE OR REPLACE TABLE allocation_vs_target AS
            SELECT
              w.month,
              w.asset_class,
              w.actual_weight,
              t.target_weight,
              w.actual_weight - t.target_weight AS variance
            FROM allocation_weights w
            LEFT JOIN target_allocation t USING(asset_class)
            ORDER BY w.month, w.asset_class;
        """)
    else:
        print("INFO: allocation_vs_target skipped (no target_allocation)")

    print("Built allocation tables")

    con.execute("""
    CREATE OR REPLACE TABLE positions_enriched_export AS
    SELECT
        as_of_date,
        month,
        account_id,
        acct_group,
        tax_bucket,
        liquidity,
        symbol,
        asset_class,
        region,
        style,
        size,
        expense_ratio,
        CAST(value AS DOUBLE) AS value
    FROM positions_enriched
    ORDER BY month, account_id, symbol
    """)

    print("Built allocation tables")
else:
    print("SKIP allocation: positions table not found")

# -------------------------
# EXPORTS (Parquet)
# -------------------------
safe_copy("monthly_cashflow", "monthly_cashflow.parquet")
safe_copy("monthly_actuals_by_category", "monthly_actuals_by_category.parquet")
safe_copy("budget_monthly", "budget_monthly.parquet")
safe_copy("category_dim", "category_dim.parquet")
safe_copy("security_dim", "security_dim.parquet")
safe_copy("month_dim", "month_dim.parquet")

safe_copy("monthly_net_worth", "monthly_net_worth.parquet")
safe_copy("monthly_net_worth_by_group", "monthly_net_worth_by_group.parquet")

safe_copy("monthly_allocation", "monthly_allocation.parquet")
safe_copy("allocation_vs_target", "allocation_vs_target.parquet")
safe_copy("positions_enriched_export", "positions_enriched_export.parquet")

print("Done.")
