# Transactions loader (warnings removed: robust boolean parsing for is_transfer)
from __future__ import annotations
import os, re, glob, hashlib
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_PATH  = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "finance.duckdb"))
TXN_DIR  = DATA_DIR / "transactions" / "normalized"
TXN_DIR.mkdir(parents=True, exist_ok=True)

STOPWORDS = {"inc","inc.","llc","llc.","co","co.","corp","corp.","ltd","ltd.","the","store","stores","company","companies"}

def normalize_merchant(text: str) -> str:
    if not isinstance(text, str): return ""
    t = re.sub(r"[^a-z0-9]+", " ", text.lower())
    toks = [w for w in t.split() if w not in STOPWORDS]
    return " ".join(toks).strip()

def to_int_cents(x) -> int:
    if pd.isna(x): return 0
    if isinstance(x, str):
        x = x.replace("$","").replace(",","").strip()
    try:
        return int(round(float(x) * 100))
    except Exception:
        return 0

def flexible_rename(df: pd.DataFrame) -> pd.DataFrame:
    mapping_candidates: Dict[str, List[str]] = {
        "date":       ["date","Date","posted","Transaction Date"],
        "description":["description","Description","merchant","Payee","Name"],
        "amount":     ["amount","Amount","amount_usd","Amount (USD)","Amount USD"],
        "account_id": ["account_id","Account","Account Id","AccountId","Acct"],
        "category":   ["category","Category"],
        "memo":       ["memo","Memo","Notes","Note"],
        "tags":       ["tags","Tags"],
        "is_transfer":["is_transfer","IsTransfer","Transfer"],
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
    grp = df.groupby(["date","account_id","amount_cents","merchant_norm"], dropna=False)
    return (grp.cumcount() + 1).astype("int64")

def make_txn_id(row: pd.Series) -> str:
    key = f"{row['date']}|{row['account_id']}|{row['amount_cents']}|{row['merchant_norm']}|{row['dup_seq']}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def _col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ? LIMIT 1",
        [table, col]).fetchone() is not None

def _find_dependents(con: duckdb.DuckDBPyConnection, base: str) -> list[tuple[str,str]]:
    rows = con.execute("SELECT * FROM duckdb_dependencies()").fetchdf()
    deps: list[tuple[str,str]] = []
    if rows is not None and len(rows) > 0:
        for _, r in rows.iterrows():
            if (str(r.get("dependson_name","")).lower() == base.lower() and
                str(r.get("dependson_type","")).upper() in {"TABLE","VIEW"}):
                deps.append((str(r.get("type","")).upper(), str(r.get("name",""))))
    return deps

def _drop_all_dependents(con: duckdb.DuckDBPyConnection, base: str = "transactions"):
    seen = set()
    queue = [base]
    while queue:
        target = queue.pop(0)
        for kind,name in _find_dependents(con, target):
            key = (kind,name)
            if key in seen: 
                continue
            seen.add(key)
            try:
                con.execute(f"DROP {kind} IF EXISTS {name};")
            except Exception:
                pass
            queue.append(name)

def _migrate_drop_subcategory(con: duckdb.DuckDBPyConnection):
    if not _col_exists(con, "transactions", "subcategory"):
        return
    _drop_all_dependents(con, "transactions")
    pragma_df = con.execute("PRAGMA table_info('transactions')").fetchdf()
    cols = [str(n) for n in pragma_df["name"].tolist() if n != "subcategory"]
    select_list = ", ".join(cols)
    con.execute(f"CREATE TABLE transactions__new AS SELECT {select_list} FROM transactions;")
    con.execute("DROP TABLE transactions;")
    con.execute("ALTER TABLE transactions__new RENAME TO transactions;")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_txnid ON transactions(txn_id);")

def _parse_is_transfer(series: pd.Series) -> pd.Series:
    # Robust, warning-free conversion to bool
    # Accepts 1/0, true/false, yes/no, y/n (case-insensitive). Anything else -> False.
    s = pd.Series(series, dtype="string").str.strip().str.lower()
    mask = s.isin(["1","true","t","yes","y"])
    return mask.fillna(False).astype(bool)

def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    con.execute("""    CREATE TABLE IF NOT EXISTS transactions (
        txn_id TEXT PRIMARY KEY,
        date DATE,
        account_id TEXT,
        amount_cents BIGINT,
        amount DOUBLE,
        description TEXT,
        merchant_norm TEXT,
        category TEXT,
        memo TEXT,
        tags TEXT,
        is_transfer BOOLEAN DEFAULT FALSE
    );
    """)

    _migrate_drop_subcategory(con)

    csvs = sorted(glob.glob(str(TXN_DIR / "**" / "*.csv"), recursive=True))
    if not csvs:
        print(f"No transaction files found in: {TXN_DIR}"); return

    total_rows = 0
    for path in csvs:
        df = pd.read_csv(path)
        df = flexible_rename(df)

        req = {"date","description","amount","account_id"}
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

        for col in ["category","memo","tags","is_transfer"]:
            if col not in df.columns: df[col] = None
        # warning-free conversion
        df["is_transfer"] = _parse_is_transfer(df["is_transfer"])

        out_cols = ["txn_id","date","account_id","amount_cents","amount",
                    "description","merchant_norm","category","memo","tags","is_transfer"]
        df = df[out_cols]
        total_rows += len(df)

        con.register("staging_df", df)
        con.execute("""        MERGE INTO transactions t
        USING staging_df s
        ON t.txn_id = s.txn_id
        WHEN MATCHED THEN UPDATE SET
            date = s.date,
            account_id = s.account_id,
            amount_cents = s.amount_cents,
            amount = s.amount,
            description = s.description,
            merchant_norm = s.merchant_norm,
            category = s.category,
            memo = s.memo,
            tags = s.tags,
            is_transfer = COALESCE(s.is_transfer, FALSE)
        WHEN NOT MATCHED THEN INSERT (txn_id, date, account_id, amount_cents, amount,
                                      description, merchant_norm, category, memo, tags, is_transfer)
        VALUES (s.txn_id, s.date, s.account_id, s.amount_cents, s.amount,
                s.description, s.merchant_norm, s.category, s.memo, s.tags,
                COALESCE(s.is_transfer, FALSE));
        """)

    cnt = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    print(f"Transactions MERGE complete. Table rows: {cnt} (processed {total_rows} staged rows across {len(csvs)} files)")

if __name__ == "__main__":
    main()
