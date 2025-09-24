# src/etl/load_rules.py
from __future__ import annotations
import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()

# Paths
REPO_ROOT = Path(__file__).resolve().parents[2]
RULES_DIR = Path(os.getenv("RULES_DIR", REPO_ROOT / "rules"))
DATA_DIR  = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH   = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))

con = duckdb.connect(str(DB_PATH))

def load_csv_table(csv_path: Path, create_sql: str, table: str):
    if not csv_path.exists():
        print(f"SKIP {table}: {csv_path} not found")
        return
    # Ensure table exists with expected schema
    con.execute(create_sql)
    # Replace all rows from CSV
    con.execute(f"DELETE FROM {table}")
    con.execute(f"INSERT INTO {table} SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(csv_path)])
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Loaded {table}: {n} rows")

# 1) account_dim
load_csv_table(
    RULES_DIR / "account_dim.csv",
    """
    CREATE TABLE IF NOT EXISTS account_dim (
      account_id TEXT PRIMARY KEY,
      account_name TEXT,
      owner TEXT,
      type TEXT,
      acct_group TEXT,
      tax_bucket TEXT,
      liquidity TEXT,
      include_networth BOOLEAN,
      include_liquid BOOLEAN
    )
    """,
    "account_dim"
)

# 2) budget_monthly
load_csv_table(
    RULES_DIR / "budget_monthly.csv",
    """
    CREATE TABLE IF NOT EXISTS budget_monthly (
      month DATE,
      category TEXT,
      amount DOUBLE
    )
    """,
    "budget_monthly"
)

# 3) category_dim (used for tagging/reporting)
load_csv_table(
    RULES_DIR / "category_dim.csv",
    """
    CREATE TABLE IF NOT EXISTS category_dim (
      category TEXT PRIMARY KEY,
      top_bucket TEXT,
      notes TEXT
    )
    """,
    "category_dim"
)

# 4) security_dim (for allocation joins)
load_csv_table(
    RULES_DIR / "security_dim.csv",
    """
    CREATE TABLE IF NOT EXISTS security_dim (
      symbol TEXT PRIMARY KEY,
      asset_class TEXT,
      region TEXT,
      style TEXT,
      size TEXT,
      expense_ratio DECIMAL(9,6)
    )
    """,
    "security_dim"
)

# 5) target_allocation (for actual vs target)
load_csv_table(
    RULES_DIR / "target_allocation.csv",
    """
    CREATE TABLE IF NOT EXISTS target_allocation (
      asset_class TEXT PRIMARY KEY,
      target_weight DOUBLE
    )
    """,
    "target_allocation"
)

print("Rules loaded.")
