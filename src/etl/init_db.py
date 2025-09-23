"""
Create (or open) finance.duckdb and ensure base tables exist.

Tables created:
- transactions: raw transaction feed (one row per transaction)

Env:
- DATA_DIR: folder where finance.duckdb lives (set in .env)

Run:
    python src/etl/init_db.py
"""

import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()  # reads DATA_DIR from .env
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
DB.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB))
con.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY,
    date DATE,
    account_id TEXT,
    amount_cents BIGINT,
    amount DOUBLE,
    description TEXT,
    merchant_norm TEXT,
    category TEXT,
    subcategory TEXT,
    memo TEXT,
    tags TEXT,
    is_transfer BOOLEAN DEFAULT FALSE
)
""")
con.close()
print(f"DB ready at {DB}")
