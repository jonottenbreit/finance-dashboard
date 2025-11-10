import duckdb

DB_PATH = r"C:\Users\jo136\OneDrive\FinanceData\finance.duckdb"

def get_con():
    return duckdb.connect(DB_PATH, read_only=False)
