# -*- coding: utf-8 -*-
r"""
Load normalized positions into DuckDB with snapshot-replace semantics per (as_of_date, account_id).

Strategy:
1) Read normalized CSVs recursively under .../positions/normalized/<vendor>/<route>/positions_YYYY-MM-DD.csv
2) Normalize keys: account_id = UPPER(TRIM(account_id)), symbol = TRIM(symbol)
3) Keep only the NEWEST row per (as_of_date, account_id, symbol) using file mtime
4) MERGE (UPSERT) rows -> updates existing & inserts new without constraint errors
5) Anti-delete: remove any positions in those snapshots that are NOT in staging (handles sells)
"""

import os, glob
from pathlib import Path
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()
DATA_DIR  = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH   = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))
NORM_ROOT = DATA_DIR / "positions" / "normalized"

con = duckdb.connect(str(DB_PATH))

# Table + unique index (idempotent)
con.execute("""
CREATE TABLE IF NOT EXISTS positions(
  as_of_date DATE,
  account_id TEXT,
  symbol TEXT,
  shares DOUBLE,
  price DOUBLE,
  market_value DOUBLE
);
""")
con.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_positions
               ON positions(as_of_date, account_id, symbol);""")

def _load_normalized_files() -> pd.DataFrame:
    files = glob.glob(str(NORM_ROOT / "**" / "positions_*.csv"), recursive=True)
    # Ignore any legacy flat files that might sit directly under .../normalized/
    files = [f for f in files if Path(f).parent != NORM_ROOT]

    if not files:
        print(f"[load_positions] No normalized files found under: {NORM_ROOT}")
        return pd.DataFrame(columns=[
            "as_of_date","account_id","symbol","shares","price","market_value","__mtime","__path"
        ])

    frames = []
    for f in files:
        p = Path(f)
        df = pd.read_csv(p)
        need = {"as_of_date","account_id","symbol","shares","price","market_value"}
        miss = need - set(df.columns)
        if miss:
            raise ValueError(f"{p} missing columns: {miss}")

        df["as_of_date"]  = pd.to_datetime(df["as_of_date"]).dt.date
        df["account_id"]  = df["account_id"].astype(str).str.strip().str.upper()
        df["symbol"]      = df["symbol"].astype(str).str.strip()
        df["shares"]      = pd.to_numeric(df["shares"], errors="coerce")
        df["price"]       = pd.to_numeric(df["price"], errors="coerce")
        df["market_value"]= pd.to_numeric(df["market_value"], errors="coerce")
        df["__mtime"]     = p.stat().st_mtime
        df["__path"]      = str(p)
        frames.append(df)

    staging = pd.concat(frames, ignore_index=True)

    # Keep only newest file per (date, account, symbol)
    staging = (
        staging.sort_values("__mtime", ascending=False)
               .drop_duplicates(subset=["as_of_date","account_id","symbol"], keep="first")
    )
    return staging

def load_all():
    staging = _load_normalized_files()
    if staging.empty:
        return

    # Snapshots (date+account) we are refreshing atomically
    keys = staging[["as_of_date","account_id"]].drop_duplicates()

    # Register for SQL
    con.register("staging_all",
        staging[["as_of_date","account_id","symbol","shares","price","market_value"]])
    con.register("keys", keys)

    print(f"[load_positions] Files staged: {staging['__path'].nunique()} | "
          f"Rows staged (deduped): {len(staging)} | "
          f"Snapshots: {len(keys)}")

    con.execute("BEGIN")

    # 1) UPSERT (update existing rows, insert new) to avoid unique key errors
    con.execute("""
        MERGE INTO positions p
        USING staging_all s
        ON  p.as_of_date = s.as_of_date
        AND p.account_id = s.account_id
        AND p.symbol     = s.symbol
        WHEN MATCHED THEN UPDATE SET
            shares = s.shares,
            price  = s.price,
            market_value = s.market_value
        WHEN NOT MATCHED THEN INSERT (as_of_date, account_id, symbol, shares, price, market_value)
        VALUES (s.as_of_date, s.account_id, s.symbol, s.shares, s.price, s.market_value);
    """)

    # 2) Anti-delete: remove any positions for those (date, account) that are NOT in staging
    #    (handles sells and guarantees exact snapshot for each date+account)
    con.execute("""
        DELETE FROM positions p
        WHERE (p.as_of_date, p.account_id) IN (SELECT as_of_date, account_id FROM keys)
          AND NOT EXISTS (
            SELECT 1 FROM staging_all s
            WHERE s.as_of_date = p.as_of_date
              AND s.account_id = p.account_id
              AND s.symbol     = p.symbol
          );
    """)

    con.execute("COMMIT")

    total = con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    print(f"[load_positions] Done. positions row count: {total}")

if __name__ == "__main__":
    load_all()
