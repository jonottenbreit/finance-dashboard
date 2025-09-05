import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()
DB = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData")) / "finance.duckdb"

with duckdb.connect(str(DB)) as con:
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_txn_id ON transactions(txn_id);")

print("Unique index ensured on transactions.txn_id.")
