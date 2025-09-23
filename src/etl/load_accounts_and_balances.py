# src/etl/load_accounts_and_balances.py
from __future__ import annotations
import os, re, glob
from pathlib import Path
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH  = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))
BAL_DIR  = DATA_DIR / "balances"
BAL_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))

# Ensure tables exist
con.execute("""
CREATE TABLE IF NOT EXISTS balance_snapshot (
  as_of_date DATE,
  account_id TEXT,
  balance DOUBLE,
  PRIMARY KEY(as_of_date, account_id)
)
""")

def infer_date_from_filename(p: Path):
    # looks for YYYY-MM-DD in file name
    m = re.search(r"(\d{4}-\d{2}-\d{2})", p.stem)
    return m.group(1) if m else None

csvs = sorted(glob.glob(str(BAL_DIR / "*.csv")))
if not csvs:
    print(f"No balance files found in: {BAL_DIR}")
    raise SystemExit(0)

total_rows = 0
for f in csvs:
    print("Loading", f)
    df = pd.read_csv(f)

    # Normalize headers
    cols = {c.lower(): c for c in df.columns}
    # preferred names
    date_col = cols.get("as_of_date")
    acct_col = cols.get("account_id") or cols.get("account")
    bal_col  = cols.get("balance")    or cols.get("amount")

    if acct_col is None or bal_col is None:
        raise ValueError(f"{f} missing required columns (need account_id/account and balance/amount)")

    df = df.rename(columns={
        acct_col: "account_id",
        bal_col:  "balance"
    })

    # Determine as_of_date
    if date_col:
        df["as_of_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    else:
        inferred = infer_date_from_filename(Path(f))
        if not inferred:
            raise ValueError(f"Could not infer date from filename: {f} (expected YYYY-MM-DD in name)")
        df["as_of_date"] = pd.to_datetime(inferred).date()

    # Coerce numeric balance
    df["balance"] = pd.to_numeric(df["balance"], errors="coerce").fillna(0.0)

    # Keep final columns only
    df = df[["as_of_date", "account_id", "balance"]]
    total_rows += len(df)

    # Upsert into DuckDB (requires PRIMARY KEY on (as_of_date, account_id))
    con.register("staging_bal", df)
    con.execute("""
        INSERT INTO balance_snapshot AS t
        SELECT as_of_date, account_id, balance
        FROM staging_bal
        ON CONFLICT (as_of_date, account_id) DO UPDATE SET
            balance = excluded.balance
    """)

cnt = con.execute("SELECT COUNT(*) FROM balance_snapshot").fetchone()[0]
print(f"Balances upsert complete. balance_snapshot rows: {cnt} (processed {total_rows} staged rows across {len(csvs)} files)")
