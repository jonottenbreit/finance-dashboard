"""
Quick inspection helper: print row counts and a small preview.

Run:
    python src/etl/peek.py
"""

import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()
DB = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData")) / "finance.duckdb"
print("DB path:", DB)
with duckdb.connect(str(DB)) as con:
    print(con.execute("SELECT COUNT(*) AS n FROM transactions").df())
    print(con.execute("SELECT * FROM transactions ORDER BY date DESC LIMIT 5").df())
