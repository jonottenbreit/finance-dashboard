"""
Load account dimension (repo/rules) and monthly balance snapshots (OneDrive DATA_DIR/balances).

CSV inputs
---------
rules/account_dim.csv
    account_id,account_name,owner,type,acct_group,tax_bucket,liquidity,include_networth,include_liquid

<DATA_DIR>/balances/*.csv
    as_of_date,account_id,balance
    (liabilities may be entered positive; we'll normalize them to negative using account_dim.type)

Creates/updates DuckDB tables
-----------------------------
account_dim(account_id TEXT PRIMARY KEY, ...)
balance_snapshot(as_of_date DATE, account_id TEXT, balance DECIMAL(18,2), currency TEXT DEFAULT 'USD',
                 PRIMARY KEY(as_of_date, account_id))

Run
---
python src/etl/load_accounts_and_balances.py
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import duckdb
from dotenv import load_dotenv

# --- Paths ---
HERE = Path(__file__).resolve()
REPO = HERE.parents[2]                # .../finance-dashboard
RULES = REPO / "rules"
ACCT_CSV = RULES / "account_dim.csv"

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB = DATA_DIR / "finance.duckdb"
BAL_DIR = DATA_DIR / "balances"


def _require_cols(df: pd.DataFrame, cols: list[str], src: Path) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{src} missing required columns: {missing}")


def read_balances() -> pd.DataFrame:
    BAL_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(BAL_DIR.glob("*.csv"))
    if not files:
        raise SystemExit(
            f"No balance CSVs found in {BAL_DIR}\n"
            "Create e.g. balance_snapshot.csv with headers: as_of_date,account_id,balance"
        )
    frames = []
    for f in files:
        df = pd.read_csv(f)
        _require_cols(df, ["as_of_date", "account_id", "balance"], f)
        df = df.copy()
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
        df["account_id"] = df["account_id"].astype(str).str.strip()
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce").fillna(0.0).round(2)
        frames.append(df[["as_of_date", "account_id", "balance"]])
    out = pd.concat(frames, ignore_index=True)
    return out


def main() -> None:
    if not ACCT_CSV.exists():
        raise SystemExit(f"Missing dimension CSV: {ACCT_CSV}")

    # --- account_dim from rules/ (source of truth) ---
    acct = pd.read_csv(ACCT_CSV).fillna("")
    _require_cols(
        acct,
        [
            "account_id",
            "account_name",
            "owner",
            "type",  # Asset | Liability
            "acct_group",
            "tax_bucket",
            "liquidity",
            "include_networth",
            "include_liquid",
        ],
        ACCT_CSV,
    )
    for c in ["account_id", "account_name", "owner", "type", "acct_group", "tax_bucket", "liquidity"]:
        acct[c] = acct[c].astype(str).str.strip()
    for c in ["include_networth", "include_liquid"]:
        acct[c] = acct[c].astype(int).clip(0, 1)

    # --- balance snapshots from OneDrive ---
    bal = read_balances()

    # Validate account IDs in balances exist in account_dim
    known = set(acct["account_id"])
    unknown = sorted(set(bal["account_id"]) - known)
    if unknown:
        raise SystemExit(f"Unknown account_id in balances: {unknown}. Add them to rules/account_dim.csv first.")

    # Normalize liabilities to negative (in case someone typed a positive)
    type_by_id = dict(zip(acct["account_id"], acct["type"]))
    bal["balance"] = bal.apply(
        lambda r: -abs(r["balance"]) if type_by_id.get(r["account_id"]) == "Liability" else float(r["balance"]),
        axis=1,
    )

    # --- Write to DuckDB ---
    with duckdb.connect(str(DB)) as con:
        # account_dim = replace with CSV (CSV is the source of truth)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS account_dim (
              account_id TEXT PRIMARY KEY,
              account_name TEXT,
              owner TEXT,
              type TEXT,               -- Asset | Liability
              acct_group TEXT,         -- Liquid | Retirement | HSA | RealEstate | Debt | RSU_Vested | RSU_Unvested | Pension | ...
              tax_bucket TEXT,         -- Taxable | PreTax | Roth | HSA | N/A
              liquidity TEXT,          -- Liquid | Semi | Illiquid
              include_networth BOOLEAN,
              include_liquid  BOOLEAN
            );
            """
        )
        con.execute("DELETE FROM account_dim;")
        con.register("acct_df", acct)
        con.execute("INSERT INTO account_dim SELECT * FROM acct_df;")

        # balance_snapshot = UPSERT using ON CONFLICT
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS balance_snapshot (
              as_of_date DATE,
              account_id TEXT,
              balance DECIMAL(18,2),
              currency TEXT DEFAULT 'USD',
              PRIMARY KEY(as_of_date, account_id)
            );
            """
        )
        con.register("bal_df", bal)
        con.execute(
            """
            INSERT INTO balance_snapshot (as_of_date, account_id, balance)
            SELECT as_of_date, account_id, balance FROM bal_df
            ON CONFLICT (as_of_date, account_id) DO UPDATE
            SET balance = EXCLUDED.balance;
            """
        )

    print(f"Loaded {len(acct)} accounts and {len(bal)} balance rows into {DB}")


if __name__ == "__main__":
    main()
