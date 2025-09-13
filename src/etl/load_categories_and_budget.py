"""
Load category dimension and monthly budget into DuckDB.

Inputs (CSV in repo/rules/):
- category_dim.csv (headers: category,parent_category,top_bucket)
- budget_monthly.csv (headers: month,category,amount)

Creates/updates DuckDB tables:
- category_dim(category TEXT PRIMARY KEY, parent_category TEXT, top_bucket TEXT)
- budget_monthly(month DATE, category TEXT, amount DECIMAL(18,2))

Run:
    python src/etl/load_categories_and_budget.py
"""
import os
from pathlib import Path
import duckdb
import pandas as pd
from dotenv import load_dotenv

# Repo paths
HERE = Path(__file__).resolve()
REPO = HERE.parents[2]                       # ...\finance-dashboard
RULES = REPO / "rules"
CAT_CSV = RULES / "category_dim.csv"
BUD_CSV = RULES / "budget_monthly.csv"

# Data/DB path
load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"

def main():
    if not CAT_CSV.exists():
        raise SystemExit(f"Missing {CAT_CSV}")
    if not BUD_CSV.exists():
        raise SystemExit(f"Missing {BUD_CSV}")

    # --- Read & clean categories ---
    cat = pd.read_csv(CAT_CSV).fillna("")
    for col in ("category", "parent_category", "top_bucket"):
        if col in cat.columns:
            cat[col] = cat[col].astype(str).str.strip()

    # --- Read & clean budget ---
    bud = pd.read_csv(BUD_CSV)
    bud["month"] = pd.to_datetime(bud["month"], errors="coerce").dt.date
    bud["category"] = bud["category"].astype(str).str.strip()
    bud["amount"] = pd.to_numeric(bud["amount"], errors="coerce").fillna(0.0).round(2)

    with duckdb.connect(str(DB)) as con:
        # Ensure tables
        con.execute("""
            CREATE TABLE IF NOT EXISTS category_dim (
                category TEXT PRIMARY KEY,
                parent_category TEXT,
                top_bucket TEXT
            );
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS budget_monthly (
                month DATE,
                category TEXT,
                amount DECIMAL(18,2)
            );
        """)
        # Replace contents (simple & explicit)
        con.execute("DELETE FROM category_dim;")
        con.execute("DELETE FROM budget_monthly;")

        # Load
        con.register("cat_df", cat)
        con.register("bud_df", bud)
        con.execute("INSERT INTO category_dim SELECT * FROM cat_df;")
        con.execute("INSERT INTO budget_monthly SELECT * FROM bud_df;")

        # Helpful index for budget lookups
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_budget ON budget_monthly(month, category);")

    print(f"Loaded {len(cat)} categories and {len(bud)} budget rows into {DB}")

if __name__ == "__main__":
    main()
