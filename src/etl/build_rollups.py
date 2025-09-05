from pathlib import Path
import os, duckdb
from dotenv import load_dotenv

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
EXPORTS = DATA_DIR / "exports"
EXPORTS.mkdir(exist_ok=True)

out_parquet = EXPORTS / "monthly_cashflow.parquet"

with duckdb.connect(str(DB)) as con:
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
    # DuckDB likes forward slashes in COPY paths
    con.execute(f"COPY monthly_cashflow TO '{out_parquet.as_posix()}' (FORMAT PARQUET);")

print("Exported:", out_parquet)
