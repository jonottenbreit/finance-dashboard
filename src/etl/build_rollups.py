"""
Build reporting rollups and export Parquet snapshots.

Exports (written under <DATA_DIR>/exports)
- monthly_cashflow.parquet
- monthly_actuals_by_category.parquet
- budget_monthly.parquet              (if table exists)
- category_dim.parquet                (if table exists)
- month_dim.parquet
- monthly_net_worth.parquet           (if account_dim + balance_snapshot exist)
- monthly_net_worth_by_group.parquet  (if account_dim + balance_snapshot exist)

Run:
    python src/etl/build_rollups.py
"""
from __future__ import annotations
import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
EXPORTS = DATA_DIR / "exports"
EXPORTS.mkdir(exist_ok=True)

def p(name: str) -> str:
    return (EXPORTS / name).as_posix()

def has_table(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    q = "SELECT 1 FROM information_schema.tables WHERE table_schema IN ('main','temp') AND table_name = ? LIMIT 1"
    return bool(con.execute(q, [table]).fetchone())

with duckdb.connect(str(DB)) as con:
    # --- monthly_cashflow from transactions (always built if transactions exists) ---
    if has_table(con, "transactions"):
        con.execute("""
            CREATE OR REPLACE TABLE monthly_cashflow AS
            SELECT
              date_trunc('month', date)::DATE AS month,
              SUM(amount)                                        AS net_cashflow,
              SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END)   AS income,
              SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)   AS spending
            FROM transactions
            GROUP BY 1
            ORDER BY 1;
        """)
    else:
        print("WARN: table 'transactions' not found; skipping monthly_cashflow.")

    # --- actuals by category (joins category_dim if present) ---
    if has_table(con, "transactions"):
        left_join = "LEFT JOIN category_dim cd ON t.category = cd.category" if has_table(con, "category_dim") else "LEFT JOIN (SELECT NULL) cd ON FALSE"
        con.execute(f"""
            CREATE OR REPLACE TABLE monthly_actuals_by_category AS
            SELECT
              date_trunc('month', t.date)::DATE                      AS month,
              COALESCE(cd.top_bucket, 'Unknown')                     AS top_bucket,
              COALESCE(cd.parent_category, cd.top_bucket)            AS parent_category,
              COALESCE(t.category, 'Uncategorized')                  AS category,
              SUM(t.amount)                                          AS actual_signed,
              SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END)   AS income,
              SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END)  AS spending
            FROM transactions t
            {left_join}
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4;
        """)
    else:
        print("WARN: table 'transactions' not found; skipping monthly_actuals_by_category.")

    # --- month_dim (union whatever sources exist) ---
    unions = []
    if has_table(con, "transactions"):
        unions.append("SELECT date_trunc('month', date)::DATE AS month FROM transactions")
    if has_table(con, "budget_monthly"):
        unions.append("SELECT month FROM budget_monthly")
    if has_table(con, "balance_snapshot"):
        unions.append("SELECT date_trunc('month', as_of_date)::DATE FROM balance_snapshot")

    if unions:
        union_sql = " UNION ALL ".join(unions)
        con.execute(f"""
            CREATE OR REPLACE TABLE month_dim AS
            SELECT DISTINCT month FROM ({union_sql}) ORDER BY month;
        """)
    else:
        # Guarantee the table exists (empty) so PBI relationships won't explode
        con.execute("CREATE TABLE IF NOT EXISTS month_dim (month DATE);")

    # --- net worth rollups (requires account_dim + balance_snapshot) ---
    if has_table(con, "account_dim") and has_table(con, "balance_snapshot"):
        con.execute("""
            CREATE OR REPLACE TABLE monthly_net_worth AS
            WITH b AS (
              SELECT date_trunc('month', bs.as_of_date)::DATE AS month,
                     bs.account_id,
                     bs.balance
              FROM balance_snapshot bs
            )
            SELECT
              b.month,
              SUM(CASE WHEN ad.include_networth THEN b.balance ELSE 0 END)                               AS net_worth,
              SUM(CASE WHEN ad.include_networth AND ad.type = 'Asset' THEN b.balance ELSE 0 END)        AS assets,
              SUM(CASE WHEN ad.include_networth AND ad.type = 'Liability' THEN -b.balance ELSE 0 END)   AS liabilities,
              SUM(CASE WHEN ad.include_liquid THEN b.balance ELSE 0 END)                                AS liquid_net_worth,
              SUM(CASE WHEN ad.include_networth AND ad.acct_group IN ('Liquid','RSU_Vested') THEN b.balance ELSE 0 END) AS investable_assets
            FROM b
            JOIN account_dim ad ON ad.account_id = b.account_id
            GROUP BY 1
            ORDER BY 1;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE monthly_net_worth_by_group AS
            WITH b AS (
              SELECT date_trunc('month', bs.as_of_date)::DATE AS month,
                     bs.account_id,
                     bs.balance
              FROM balance_snapshot bs
            )
            SELECT
              b.month,
              ad.acct_group,
              SUM(CASE WHEN ad.include_networth THEN b.balance ELSE 0 END) AS value
            FROM b
            JOIN account_dim ad ON ad.account_id = b.account_id
            GROUP BY 1,2
            ORDER BY 1,2;
        """)
    else:
        print("INFO: net worth skipped (need both 'account_dim' and 'balance_snapshot').")

    # --- Exports (COPY only if the table exists) ---
    def safe_copy(table: str, filename: str) -> None:
        if has_table(con, table):
            con.execute(f"COPY {table} TO '{p(filename)}' (FORMAT PARQUET);")
        else:
            print(f"SKIP export: table '{table}' not found.")

    safe_copy("monthly_cashflow", "monthly_cashflow.parquet")
    if has_table(con, "budget_monthly"):
        con.execute(f"COPY (SELECT * FROM budget_monthly) TO '{p('budget_monthly.parquet')}' (FORMAT PARQUET);")
    if has_table(con, "category_dim"):
        con.execute(f"COPY (SELECT * FROM category_dim)   TO '{p('category_dim.parquet')}'   (FORMAT PARQUET);")
    safe_copy("monthly_actuals_by_category", "monthly_actuals_by_category.parquet")
    safe_copy("month_dim", "month_dim.parquet")
    safe_copy("monthly_net_worth", "monthly_net_worth.parquet")
    safe_copy("monthly_net_worth_by_group", "monthly_net_worth_by_group.parquet")

print("Exports written to:", EXPORTS)
