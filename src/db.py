# src/db.py

import duckdb

DB_PATH = r"C:\Users\jo136\OneDrive\FinanceData\finance.duckdb"


def run(sql: str):
    """
    Run SQL against the finance.duckdb database.
    Prints results if the final statement returns rows.
    """
    con = duckdb.connect(DB_PATH, read_only=False)
    try:
        result = con.execute(sql)

        # Attempt to fetch rows (works for SELECT/SHOW/PRAGMA)
        try:
            df = result.df()
            if not df.empty:
                print(df)
            else:
                print("Query executed. No rows returned.")
        except Exception:
            # For non-SELECT statements (ALTER, UPDATE, etc.)
            print("Statement executed.")

    finally:
        con.close()


# --------------------------------------------------------------------
# ADD YOUR SQL BELOW THIS LINE — ONLY EDIT THE BLOCK BELOW EACH TIME
# --------------------------------------------------------------------

# Example (REMOVE this and paste your real SQL):
# --- ADD YOUR SQL BELOW THIS LINE ---

run("""
SELECT symbol, federal_taxable_ratio, state_taxable_ratio
FROM security_dim
ORDER BY symbol;
""")

