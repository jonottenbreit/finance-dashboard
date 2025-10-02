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
    """Ensure table exists with expected schema, then replace rows from CSV."""
    if not csv_path.exists():
        print(f"SKIP {table}: {csv_path} not found")
        return
    con.execute(create_sql)
    con.execute(f"DELETE FROM {table}")
    con.execute(f"INSERT INTO {table} SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(csv_path)])
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Loaded {table}: {n} rows")

def load_category_dim():
    """Recreate category_dim to match the CSV (no subcategory) and index it."""
    csv_path = RULES_DIR / "category_dim.csv"
    if not csv_path.exists():
        print(f"SKIP category_dim: {csv_path} not found")
        return

    # Recreate the table directly from the CSV to guarantee schema alignment
    con.execute("""
        CREATE OR REPLACE TABLE category_dim AS
        SELECT
            TRIM(category)                                 AS category,
            TRIM(COALESCE(parent_category, ''))           AS parent_category,
            TRIM(COALESCE(top_bucket, ''))                AS top_bucket,
            COALESCE(notes, '')                           AS notes,
            CASE LOWER(CAST(exclude_from_budget AS VARCHAR))
                WHEN '1' THEN TRUE
                WHEN 'true' THEN TRUE
                ELSE FALSE
            END                                           AS exclude_from_budget,
            CASE LOWER(CAST(is_transfer AS VARCHAR))
                WHEN '1' THEN TRUE
                WHEN 'true' THEN TRUE
                ELSE FALSE
            END                                           AS is_transfer
        FROM read_csv_auto(?, HEADER=TRUE);
    """, [str(csv_path)])

    # Indexes consistent with the new schema
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_category_dim ON category_dim(category);")
    con.execute("CREATE INDEX IF NOT EXISTS ix_category_dim_parent ON category_dim(parent_category);")

    n = con.execute("SELECT COUNT(*) FROM category_dim").fetchone()[0]
    print(f"Loaded category_dim: {n} rows")

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

# 3) category_dim (no subcategory)
load_category_dim()

# 4) security_dim
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

# 5) target_allocation
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

# 6) category_rules (always reload so CSV edits take effect)
catrules_csv = RULES_DIR / "category_rules.csv"
if catrules_csv.exists():
    con.execute("CREATE OR REPLACE TABLE category_rules AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(catrules_csv)])
    # Optional: light index to speed contains-matching (helps a bit for many rules)
    con.execute("CREATE INDEX IF NOT EXISTS ix_category_rules_pattern ON category_rules(pattern);")
    print(f"Loaded category_rules from {catrules_csv}")
else:
    print(f"SKIP category_rules: {catrules_csv} not found")

# 7) category_overrides (manual one-off categorization rules)
overrides_csv = RULES_DIR / "category_overrides.csv"
if overrides_csv.exists():
    con.execute("""
        CREATE TABLE IF NOT EXISTS category_overrides (
            active BOOLEAN,
            date DATE,
            description_regex TEXT,
            amount DOUBLE,
            category TEXT,
            subscription BOOLEAN
        )
    """)
    con.execute("ALTER TABLE category_overrides ADD COLUMN IF NOT EXISTS subscription BOOLEAN;")
    con.execute("DELETE FROM category_overrides")
    con.execute("""
    INSERT INTO category_overrides
        (active, date, description_regex, amount, category, subscription)
    SELECT
        CASE
        WHEN LOWER(NULLIF(TRIM(CAST(active AS VARCHAR)), '')) IN ('1','true','t','yes','y') THEN TRUE
        WHEN LOWER(NULLIF(TRIM(CAST(active AS VARCHAR)), '')) IN ('0','false','f','no','n') THEN FALSE
        ELSE FALSE
        END AS active,
        COALESCE(
        TRY_CAST(NULLIF(TRIM(CAST("date" AS VARCHAR)), '') AS DATE),
        CAST(TRY_STRPTIME(NULLIF(TRIM(CAST("date" AS VARCHAR)), ''), '%m/%d/%Y') AS DATE)
        ) AS date,
        TRIM(CAST(description_regex AS VARCHAR)) AS description_regex,
        TRY_CAST(
        NULLIF(TRIM(REPLACE(REPLACE(CAST(amount AS VARCHAR), '$', ''), ',', '')), '')
        AS DOUBLE
        ) AS amount,
        TRIM(CAST(category AS VARCHAR)) AS category,
        CASE
        WHEN LOWER(NULLIF(TRIM(CAST(subscription AS VARCHAR)), '')) IN ('1','true','t','yes','y') THEN TRUE
        WHEN LOWER(NULLIF(TRIM(CAST(subscription AS VARCHAR)), '')) IN ('0','false','f','no','n') THEN FALSE
        ELSE FALSE
        END AS subscription
    FROM read_csv_auto(?, HEADER=TRUE)
    """, [str(overrides_csv)])


    n = con.execute("SELECT COUNT(*) FROM category_overrides").fetchone()[0]
    print(f"Loaded category_overrides: {n} rows")
else:
    print(f"SKIP category_overrides: {overrides_csv} not found")


