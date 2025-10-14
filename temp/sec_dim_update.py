"""
update_security_dim_cols.py

One-shot (idempotent) migration script to:
  1) Ensure security_dim has `dividend_yield` and `qualified_ratio` columns.
  2) Optionally sync JUST those two fields from rules/security_dim.csv into the DB.

Notes:
- We DO NOT set/guess any values in SQL. Your CSV is the source of truth.
- If the CSV doesn’t have those columns, nothing breaks—existing DB rows remain as-is (NULLs allowed).
- Symbol match is case-insensitive.
"""

from __future__ import annotations
import os
from pathlib import Path
import duckdb

# --- CONFIG ---
# Environment overrides are supported; otherwise sensible defaults:
REPO_ROOT = Path(__file__).resolve().parent
RULES_DIR = Path(os.getenv("RULES_DIR", REPO_ROOT / "rules"))
DATA_DIR  = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH   = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))

SECURITY_CSV = RULES_DIR / "security_dim.csv"  # optional; will be used if present

def main() -> None:
    print(f"Connecting to DuckDB at: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))

    # 1) Add the columns (safe to run multiple times)
    print("Ensuring columns exist on security_dim ...")
    con.execute("""
        ALTER TABLE security_dim ADD COLUMN IF NOT EXISTS dividend_yield DOUBLE;
    """)
    con.execute("""
        ALTER TABLE security_dim ADD COLUMN IF NOT EXISTS qualified_ratio DOUBLE;
    """)
    print("Columns ensured.")

    # 2) Optionally sync from CSV into DB, by symbol (case-insensitive)
    if SECURITY_CSV.exists():
        print(f"Found CSV → syncing columns from: {SECURITY_CSV}")
        # Create a temp view to read CSV with headers; we only care about the 3 columns.
        # If CSV doesn’t include the new cols, the UPDATE will be a no-op.
        con.execute("""
            CREATE OR REPLACE TEMP VIEW v_sec_csv AS
            SELECT *
            FROM read_csv_auto(?, HEADER=TRUE);
        """, [str(SECURITY_CSV)])

        # Sanity: check which columns are present
        csv_cols = {r[0].lower() for r in con.execute("DESCRIBE v_sec_csv").fetchall()}
        need_cols = {"symbol", "dividend_yield", "qualified_ratio"}
        missing = need_cols - csv_cols
        if {"symbol"} - csv_cols:
            print("ERROR: CSV must contain a 'symbol' column to sync. Aborting CSV sync.")
        else:
            # Build dynamic SET clause only for columns that exist in the CSV
            set_clauses = []
            if "dividend_yield" in csv_cols:
                set_clauses.append("d.dividend_yield = c.dividend_yield")
            if "qualified_ratio" in csv_cols:
                set_clauses.append("d.qualified_ratio = c.qualified_ratio")

            if set_clauses:
                set_sql = ", ".join(set_clauses)
                sql = f"""
                    UPDATE security_dim AS d
                    SET {set_sql}
                    FROM (
                        SELECT
                            symbol,
                            { "dividend_yield," if "dividend_yield" in csv_cols else "" }
                            { "qualified_ratio" if "qualified_ratio" in csv_cols else "" }
                        FROM v_sec_csv
                    ) AS c
                    WHERE lower(d.symbol) = lower(c.symbol)
                """
                # Clean up potential trailing commas if only one column exists
                sql = sql.replace("dividend_yield,\n                            \n", "dividend_yield\n")
                sql = sql.replace(",,", ",")

                con.execute(sql)
                updated = con.execute("SELECT changes()").fetchone()[0]
                print(f"Updated rows from CSV: {updated}")
            else:
                print("CSV has no 'dividend_yield' or 'qualified_ratio' columns. Nothing to sync.")
    else:
        print(f"No CSV found at {SECURITY_CSV}. Skipping CSV sync. (DB columns are in place.)")

    # 3) Quick report
    print("\nSample check (top 20 with any yield/ratio present):")
    out = con.execute("""
        SELECT symbol, dividend_yield, qualified_ratio
        FROM security_dim
        WHERE dividend_yield IS NOT NULL OR qualified_ratio IS NOT NULL
        ORDER BY symbol
        LIMIT 20;
    """).fetchdf()
    try:
        # Pretty display if pandas is available
        import pandas as pd  # noqa
        print(out.to_string(index=False))
    except Exception:
        print(out)

    con.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
