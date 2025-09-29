"""
Robust transactions loader (multi-file, idempotent).

- Reads all CSVs in DATA_DIR/transactions/
- Flexible column mapping
- Computes amount_cents, merchant_norm, dup_seq, and stable txn_id
- Upserts into DuckDB
"""

from __future__ import annotations
import os, re, glob, hashlib
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd
from dotenv import load_dotenv

# -------------------------
# ENV / PATHS
# -------------------------
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH  = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))
TXN_DIR = DATA_DIR / "transactions" / "normalized"
TXN_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# HELPERS
# -------------------------
STOPWORDS = {
    "inc","inc.","llc","llc.","co","co.","corp","corp.","ltd","ltd.","the",
    "store","stores","company","companies"
}

def normalize_merchant(text: str) -> str:
    if not isinstance(text, str): return ""
    t = re.sub(r"[^a-z0-9]+", " ", text.lower())
    toks = [w for w in t.split() if w not in STOPWORDS]
    return " ".join(toks).strip()

def to_int_cents(x) -> int:
    if pd.isna(x): return 0
    if isinstance(x, str):
        x = x.replace("$", "").replace(",", "").strip()
    try:
        return int(round(float(x) * 100))
    except Exception:
        return 0

def flexible_rename(df: pd.DataFrame) -> pd.DataFrame:
    mapping_candidates: Dict[str, List[str]] = {
        "date":       ["date", "Date", "posted", "Transaction Date"],
        "description":["description","Description","merchant","Payee","Name"],
        "amount":     ["amount","Amount","amount_usd","Amount (USD)","Amount USD"],
        "account_id": ["account_id","Account","Account Id","AccountId","Acct"],
        "category":   ["category","Category"],
        "subcategory":["subcategory","Subcategory","Sub Category"],
        "memo":       ["memo","Memo","Notes","Note"],
        "dup_hint":   ["dup_seq","duplicate","Dupe","Instance"]
    }
    colmap = {}
    lower_cols = {c.lower(): c for c in df.columns}
    for target, candidates in mapping_candidates.items():
        for cand in candidates:
            if cand in df.columns:
                colmap[cand] = target; break
            lc = cand.lower()
            if lc in lower_cols:
                colmap[lower_cols[lc]] = target; break
    return df.rename(columns=colmap)

def compute_dup_seq(df: pd.DataFrame) -> pd.Series:
    if "dup_hint" in df.columns:
        s = pd.to_numeric(df["dup_hint"], errors="coerce").fillna(1).astype(int)
        return s.clip(lower=1)
    grp = df.groupby(["date", "account_id", "amount_cents", "merchant_norm"], dropna=False)
    return grp.cumcount() + 1

def make_txn_id(row: pd.Series) -> str:
    key = f"{row['date']}|{row['account_id']}|{row['amount_cents']}|{row['merchant_norm']}|{row['dup_seq']}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

# -------------------------
# MAIN
# -------------------------
def main() -> None:
    con = duckdb.connect(str(DB_PATH))

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
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_txnid ON transactions(txn_id)")

    csvs = sorted(glob.glob(str(TXN_DIR / "**" / "*.csv"), recursive=True))
    if not csvs:
        print(f"No transaction files found in: {TXN_DIR}")
        return

    total_rows = 0
    for path in csvs:
        print(f"Loading {path}")
        df = pd.read_csv(path)
        df = flexible_rename(df)

        req = {"date", "description", "amount", "account_id"}
        missing = req - set(df.columns)
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df[~df["date"].isna()]

        df["amount_cents"] = df["amount"].apply(to_int_cents).astype("int64")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df["merchant_norm"] = df["description"].astype(str).apply(normalize_merchant)
        df["dup_seq"] = compute_dup_seq(df)
        df["txn_id"] = df.apply(make_txn_id, axis=1)

        for col in ["category","subcategory","memo","tags"]:
            if col not in df.columns:
                df[col] = None

        out_cols = [
            "txn_id","date","account_id","amount_cents","amount",
            "description","merchant_norm","category","subcategory","memo","tags"
        ]
        df = df[out_cols]
        total_rows += len(df)

        con.register("staging_df", df)
        con.execute("""
        INSERT INTO transactions AS t
        SELECT
            txn_id, date, account_id, amount_cents, amount,
            description, merchant_norm, category, subcategory, memo, tags, FALSE AS is_transfer
        FROM staging_df
        ON CONFLICT (txn_id) DO UPDATE SET
            date          = excluded.date,
            account_id    = excluded.account_id,
            amount_cents  = excluded.amount_cents,
            amount        = excluded.amount,
            description   = excluded.description,
            merchant_norm = excluded.merchant_norm,
            category      = excluded.category,
            subcategory   = excluded.subcategory,
            memo          = excluded.memo,
            tags          = excluded.tags
        """)

    cnt = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    print(f"Transactions MERGE complete. Table rows: {cnt} (processed {total_rows} staged rows across {len(csvs)} files)")

if __name__ == "__main__":
    main()
