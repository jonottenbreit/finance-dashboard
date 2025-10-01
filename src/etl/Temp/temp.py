import duckdb

DB = r"C:/Users/jo136/OneDrive/FinanceData/finance.duckdb"
NORM_BASE = r"C:/Users/jo136/OneDrive/FinanceData/normalized"

con = duckdb.connect(DB)

# Drop downstream views so nothing pins old state
con.execute("DROP VIEW IF EXISTS transactions_with_category;")
con.execute("DROP VIEW IF EXISTS transactions_deduped;")

# Recreate transactions from normalized parquet (top level + one subfolder like 'amazon'/'sapphire')
con.execute(f"""
CREATE OR REPLACE TABLE transactions AS
SELECT * FROM read_parquet('{NORM_BASE}/*.parquet')
UNION ALL
SELECT * FROM read_parquet('{NORM_BASE}/*/*.parquet');
""")

print("Reloaded transactions:", con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
