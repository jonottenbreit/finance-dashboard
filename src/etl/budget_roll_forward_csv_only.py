#!/usr/bin/env python3
r"""
budget_roll_forward_csv_only.py — CSV-only monthly budget roll-forward with GAP FILL.

Reads:
  --budgets : path to budgets.csv   (columns: parent_category, yyyymm, budget_amount)
  --tx      : path to transactions_with_category.parquet  (used ONLY to get MAX(date))

What it does:
  * Determine target month (YYYYMM) from max transaction date (or --target-yyyymm).
  * For each parent_category:
      - Identify all explicit budget rows <= target (sorted).
      - Forward-fill ANY missing months between explicit rows.
      - Then forward-fill from the last explicit month up to the target month.
  * Writes updated budgets.csv (unless --dry-run).

Example (PowerShell):
  python src/etl/budget_roll_forward_csv_only.py ^
    --budgets "C:\Users\jo136\OneDrive\FinanceData\budgets\budgets.csv" ^
    --tx "C:\Users\jo136\OneDrive\FinanceData\exports\transactions_with_category.parquet"
"""
import argparse
import pandas as pd
from pathlib import Path
from datetime import date

REQUIRED_BUDGET_COLS = ["parent_category", "yyyymm", "budget_amount"]

def read_budgets(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"budgets.csv not found: {path}")
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_BUDGET_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"budgets.csv missing columns: {missing}")
    df["parent_category"] = df["parent_category"].astype(str).str.strip()
    df["yyyymm"] = df["yyyymm"].astype(int)
    df["budget_amount"] = df["budget_amount"].astype(float)
    return df

def read_max_tx_month(parquet_path: Path) -> int:
    if not parquet_path.exists():
        raise FileNotFoundError(f"transactions parquet not found: {parquet_path}")
    df = pd.read_parquet(parquet_path, columns=["date"])
    if "date" not in df.columns:
        raise ValueError("transactions parquet must include a 'date' column (YYYY-MM-DD)")
    d = pd.to_datetime(df["date"], errors="coerce")
    if d.notna().any():
        mx = d.max().date()
        return int(f"{mx.year}{mx.month:02d}")
    # fallback to current month if nothing parseable
    today = date.today()
    return int(f"{today.year}{today.month:02d}")

def next_month(yyyymm: int) -> int:
    y = yyyymm // 100
    m = yyyymm % 100
    if m == 12:
        return (y + 1) * 100 + 1
    return y * 100 + (m + 1)

def iter_months(start_yyyymm: int, end_yyyymm: int):
    """Yield months after start up to and including end, e.g., 202507 -> 202510 yields 202508,202509,202510."""
    cur = start_yyyymm
    while cur < end_yyyymm:
        cur = next_month(cur)
        yield cur

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budgets", required=True, help="Path to budgets.csv")
    ap.add_argument("--tx", required=True, help="Path to transactions_with_category.parquet")
    ap.add_argument("--target-yyyymm", type=int, default=None, help="Override target month (YYYYMM)")
    ap.add_argument("--dry-run", action="store_true", help="Preview only; do not write CSV")
    args = ap.parse_args()

    budgets_path = Path(args.budgets)
    tx_path = Path(args.tx)

    bdf = read_budgets(budgets_path)
    target = args.target_yyyymm if args.target_yyyymm else read_max_tx_month(tx_path)

    cats = sorted(bdf["parent_category"].unique().tolist())
    additions = []

    for cat in cats:
        # explicit rows for this cat up to target
        sub = bdf[bdf["parent_category"] == cat].sort_values("yyyymm")
        sub_le = sub[sub["yyyymm"] <= target]

        if sub_le.empty:
            # no prior budget known; skip
            print(f"[WARN] No explicit budget for '{cat}' on/before {target}; skipping.")
            continue

        # We'll walk through the explicit months and fill gaps between them.
        expl_months = sub_le["yyyymm"].tolist()
        expl_amounts = sub_le["budget_amount"].tolist()

        # Fill gaps between explicit rows
        for i in range(len(expl_months) - 1):
            left_m = expl_months[i]
            left_amt = expl_amounts[i]
            right_m = expl_months[i + 1]
            if right_m > left_m + 1:  # there is at least one missing month
                for gap_m in iter_months(left_m, right_m):
                    # Only add if not already present (maybe user pre-filled)
                    if not ((bdf["parent_category"] == cat) & (bdf["yyyymm"] == gap_m)).any():
                        additions.append({"parent_category": cat, "yyyymm": gap_m, "budget_amount": left_amt})

        # Fill from last explicit month up to target
        last_m = expl_months[-1]
        last_amt = expl_amounts[-1]
        if last_m < target:
            for m in iter_months(last_m, target):
                if not ((bdf["parent_category"] == cat) & (bdf["yyyymm"] == m)).any():
                    additions.append({"parent_category": cat, "yyyymm": m, "budget_amount": last_amt})

    if not additions:
        print(f"[OK] No gaps to fill up to {target}.")
        return

    add_df = pd.DataFrame(additions, columns=REQUIRED_BUDGET_COLS).sort_values(["parent_category", "yyyymm"])
    # Show a concise summary
    added_count = add_df.shape[0]
    print(f"[INFO] Filling {added_count} month(s) up to {target}. Examples:")
    print(add_df.groupby("parent_category").head(3).to_string(index=False))

    if args.dry_run:
        print("[DRY] No changes written.")
        return

    out = (pd.concat([bdf, add_df], ignore_index=True)
            .drop_duplicates(subset=["parent_category","yyyymm"], keep="last")
            .sort_values(["yyyymm","parent_category"]))

    out.to_csv(budgets_path, index=False)
    print(f"[WRITE] Updated budgets CSV: {budgets_path} (added {added_count} rows)")

if __name__ == "__main__":
    main()
