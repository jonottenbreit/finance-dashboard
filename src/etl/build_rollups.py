from pathlib import Path
import os, duckdb
from dotenv import load_dotenv

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
EXPORTS = DATA_DIR / "exports"
EXPORTS.mkdir(exist_ok=True)

def p(path: Path) -> str:
    return path.as_posix()

with duckdb.connect(str(DB)) as con:
    # 1) Monthly cashflow (what you already had)
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

    # 2) Actuals by month + category (raw signs in actual_signed)
    con.execute("""
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
        LEFT JOIN category_dim cd ON t.category = cd.category
        GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4;
    """)

    # 3) Month dimension to relate budget & actuals cleanly in PBI
    con.execute("""
        CREATE OR REPLACE TABLE month_dim AS
        SELECT DISTINCT month FROM (
            SELECT date_trunc('month', date)::DATE AS month FROM transactions
            UNION ALL
            SELECT month FROM budget_monthly
        )
        ORDER BY month;
    """)

    # 4) Exports (Parquet)
    con.execute(f"COPY monthly_cashflow               TO '{p(EXPORTS / 'monthly_cashflow.parquet')}' (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM budget_monthly) TO '{p(EXPORTS / 'budget_monthly.parquet')}'   (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM category_dim)   TO '{p(EXPORTS / 'category_dim.parquet')}'     (FORMAT PARQUET);")
    con.execute(f"COPY monthly_actuals_by_category     TO '{p(EXPORTS / 'monthly_actuals_by_category.parquet')}' (FORMAT PARQUET);")
    con.execute(f"COPY month_dim                       TO '{p(EXPORTS / 'month_dim.parquet')}'       (FORMAT PARQUET);")

print("Exports written to:", EXPORTS)
