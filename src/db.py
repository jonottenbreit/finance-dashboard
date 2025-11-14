import duckdb

DB_PATH = r"C:\Users\jo136\OneDrive\FinanceData\finance.duckdb"

def get_con():
    return duckdb.connect(DB_PATH, read_only=False)

# --- EXAMPLE: run SQL here ---
sql = """
ALTER TABLE security_dim ADD COLUMN federal_taxable_ratio DOUBLE;
ALTER TABLE security_dim ADD COLUMN state_taxable_ratio DOUBLE;
"""

with get_con() as con:
    con.execute(sql)
    print(df)
