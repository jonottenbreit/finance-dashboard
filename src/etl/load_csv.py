"""
Load a CSV of transactions into DuckDB (idempotent).

What it does:
- Reads CSV at DATA_DIR/sample_transactions.csv
- Normalizes headers and types
- Builds a stable txn_id from (date, account_id, merchant_norm, amount_cents, dup_seq)
- INSERT ... ON CONFLICT (txn_id) DO NOTHING

Requires:
- Unique index on transactions(txn_id) (created if missing)

Inputs:
- DATA_DIR/sample_transactions.csv

Outputs:
- Rows inserted into transactions

Run:
    python src/etl/load_csv.py
"""

import os, hashlib
from pathlib import Path
import duckdb, pandas as pd
from dotenv import load_dotenv

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
CSV = DATA_DIR / "sample_transactions.csv"

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Map common column names → our standard
    cols = {c.lower().strip(): c for c in df.columns}
    date_col = next(k for k in cols if k in ("date", "posted date", "transaction date"))
    desc_col = next(k for k in cols if k in ("description", "merchant", "payee"))
    amt_col  = next(k for k in cols if k in ("amount", "amt", "transaction amount"))
    acct_col = next((k for k in cols if k in ("account", "account name", "account id")), None)
    cat_col  = next((k for k in cols if k in ("category", "cat")), None)
    sub_col  = next((k for k in cols if k in ("subcategory", "sub-cat", "sub category")), None)
    memo_col = next((k for k in cols if k in ("memo", "notes", "note")), None)

    out = pd.DataFrame({
        "date": pd.to_datetime(df[cols[date_col]], errors="coerce").dt.date,
        "merchant": df[cols[desc_col]].astype(str).str.strip(),
        "amount": pd.to_numeric(df[cols[amt_col]], errors="coerce").fillna(0.0).round(2),
        "account_id": (df[cols[acct_col]].astype(str).str.strip() if acct_col else "unknown"),
        "category": (df[cols[cat_col]].astype(str).str.strip() if cat_col else None),
        "subcategory": (df[cols[sub_col]].astype(str).str.strip() if sub_col else None),
        "memo": (df[cols[memo_col]].astype(str).str.strip() if memo_col else None),
    })

    # ---- Build a stable identity (NO memo). ----
    # 1) Normalize merchant to reduce noise
    out["merchant_norm"] = (
        out["merchant"]
        .str.lower()
        .str.replace(r"[^a-z0-9 ]+", "", regex=True)  # drop punctuation
        .str.replace(r"\s+", " ", regex=True)         # collapse whitespace
        .str.strip()
    )

    # 2) Amount in cents (int) to avoid float noise
    out["amount_cents"] = (out["amount"] * 100).round().astype("Int64")

    # 3) Tie-breaker for truly identical charges same day
    out["dup_seq"] = out.groupby(
        ["date", "account_id", "merchant_norm", "amount_cents"]
    ).cumcount()

    # 4) Deterministic txn_id from STABLE fields
    def _id(row):
        raw = f"{row.date}|{row.account_id}|{row.merchant_norm}|{row.amount_cents}|{row.dup_seq}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    out["txn_id"] = out.apply(_id, axis=1)

    # Final columns for the table (extra helper cols are not inserted)
    out["tags"] = None
    return out[["txn_id", "date", "account_id", "merchant", "memo",
                "amount", "category", "subcategory", "tags"]]

def main():
    if not CSV.exists():
        raise SystemExit(f"Missing CSV: {CSV}")

    df = pd.read_csv(CSV)
    df = normalize(df)

    with duckdb.connect(str(DB)) as con:
        # Ensure the unique index exists so ON CONFLICT works (safe to run every time)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_txn_id ON transactions(txn_id);")

        con.execute("CREATE TEMP TABLE t AS SELECT * FROM df")

        # Insert new rows only
        con.execute("""
            INSERT INTO transactions
            SELECT * FROM t
            ON CONFLICT (txn_id) DO NOTHING
        """)

    print(f"Loaded {len(df)} rows from {CSV.name}")

if __name__ == "__main__":
    main()
