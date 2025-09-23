import os
import glob
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load env variables
load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", "C:/Users/jo136/OneDrive/FinanceData"))
DB_PATH = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))

con = duckdb.connect(str(DB_PATH))

# Ensure tables exist
con.execute("""
CREATE TABLE IF NOT EXISTS positions (
    as_of_date DATE,
    account_id TEXT,
    symbol TEXT,
    shares DOUBLE,
    price DOUBLE,
    market_value DOUBLE
)
""")

# Clear existing (idempotent load: replace everything from CSVs)
con.execute("DELETE FROM positions")

# Load all CSVs under positions/
positions_dir = DATA_DIR / "positions"
csv_files = glob.glob(str(positions_dir / "*.csv"))

for f in csv_files:
    print(f"Loading {f}")
    df = pd.read_csv(f)
    # Basic schema enforcement
    expected_cols = {"as_of_date","account_id","symbol","shares","price","market_value"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"{f} missing cols: {missing}")
    con.execute("INSERT INTO positions SELECT * FROM df")

print(
    "Positions loaded. Row count:",
    con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
)
