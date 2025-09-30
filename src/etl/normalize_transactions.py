# -*- coding: utf-8 -*-
"""
Normalize raw bank & credit card transactions into canonical CSV:
  date, account_id, amount, description, category, memo, tags

Input  : DATA_DIR/transactions/raw/<vendor>/<route>/*-transactions.(csv|xlsx)
Output : DATA_DIR/transactions/normalized/<vendor>/<route>/transactions_<from>_to_<to>.csv

Notes
-----
* Dedupes within a batch AND against all existing normalized files for the same vendor/route.
  Dupe key = (date, account_id, amount, normalized_description).
* Category is assigned by rules in rules/category_rules.csv (optional).
* This version removes 'subcategory' entirely to align with the repo's schema change.
"""
from __future__ import annotations
import os, glob, re
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DATA_DIR   = Path(os.getenv("DATA_DIR", r"C:\\Users\\jo136\\OneDrive\\FinanceData"))
RAW_ROOT   = DATA_DIR / "transactions" / "raw"
NORM_ROOT  = DATA_DIR / "transactions" / "normalized"

REPO_ROOT = Path(os.getenv("REPO_ROOT", Path(__file__).resolve().parents[2]))
RULES_DIR = REPO_ROOT / "rules"

NORM_ROOT.mkdir(parents=True, exist_ok=True)

# Optional explicit overrides → set to match account_dim if you want controlled IDs
ACCOUNT_ID_OVERRIDES: Dict[tuple[str, str], str] = {
    ("chase", "checking"): "CHECKING_JON",
    ("chase", "savings"):  "SAVINGS_JON",
    ("chase", "sapphire"): "SAPPHIRE",
    ("chase", "amazon"):   "AMAZON",
    # add more as needed
}

# ---------- helpers ----------
STOPWORDS = {"inc","inc.","llc","llc.","co","co.","corp","corp.","ltd","ltd.","the","store","stores","company","companies"}

def _vendor_route_from_path(p: Path) -> tuple[str, str]:
    parts = [s.lower() for s in p.parts]
    try:
        i = parts.index("raw")
        vendor = parts[i+1] if i+1 < len(parts)-1 else "unknown"
        # everything after vendor up to filename is the 'route' (e.g., 'sapphire', 'amazon', 'checking')
        route  = "/".join(parts[i+2:-1]) or "unknown"
        return vendor, route
    except ValueError:
        return "unknown", "unknown"

def _account_id_from(vendor: str, route: str) -> str:
    key = (vendor.lower(), route.lower())
    if key in ACCOUNT_ID_OVERRIDES:
        return ACCOUNT_ID_OVERRIDES[key]
    # default to the route name uppercased with slashes replaced
    return route.replace("/", "_").upper()

def _num_amount(x) -> Optional[float]:
    if pd.isna(x): return None
    s = str(x).strip().replace(",", "").replace("$", "")
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").strip()
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None

def _normalize_merchant(text: str) -> str:
    if not isinstance(text, str): return ""
    t = re.sub(r"[^a-z0-9]+", " ", text.lower())
    toks = [w for w in t.split() if w not in STOPWORDS]
    return " ".join(toks).strip()

def _coalesce(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns: return cand
        lc = cand.lower()
        if lc in lower: return lower[lc]
    for col in df.columns:
        for cand in candidates:
            if cand.lower() in col.lower():
                return col
    return None

def _dupe_key_series(df: pd.DataFrame) -> pd.Series:
    desc_norm = df["description"].fillna("").map(_normalize_merchant)
    return (df["date"].astype(str) + "|" +
            df["account_id"].astype(str) + "|" +
            df["amount"].round(2).astype(str) + "|" +
            desc_norm)

# ---------- rules (category only) ----------
def _load_category_rules():
    path = RULES_DIR / "category_rules.csv"
    if not path.exists():
        print(f"[normalize/tx] No category_rules.csv at {path}; leaving category blank.")
        return None
    df = pd.read_csv(path).fillna("")
    df["priority"]    = pd.to_numeric(df.get("priority", 1000), errors="coerce").fillna(1000).astype(int)
    df["match_type"]  = df.get("match_type", "contains").str.lower().str.strip()
    df["pattern"]     = df.get("pattern", "").astype(str)
    df["category"]    = df.get("category", "").astype(str).str.strip()
    df["sign"]        = df.get("sign", "any").str.lower().str.strip().replace({"": "any"})
    df["account_id"]  = df.get("account_id", "").astype(str).str.strip().replace({"": None})
    return df.sort_values(["priority"]).reset_index(drop=True)

def _apply_category_rules(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    if rules is None or rules.empty:
        return df
    df = df.copy()
    df["merchant_norm"] = df["description"].apply(_normalize_merchant)
    df["category"] = df.get("category", None)
    remaining = pd.Series(True, index=df.index)

    for _, r in rules.iterrows():
        mtype, pat, sign, acct = r["match_type"], str(r["pattern"]), r["sign"], r["account_id"]
        cat = r["category"]
        mask = remaining.copy()
        if acct:
            mask &= (df["account_id"].astype(str).str.upper() == acct.upper())
        if sign == "positive":
            mask &= (df["amount"] > 0)
        elif sign == "negative":
            mask &= (df["amount"] < 0)

        if mtype == "contains":
            mask &= df["description"].str.contains(pat, case=False, na=False)
        elif mtype == "regex":
            mask &= df["description"].str.contains(pat, flags=re.IGNORECASE, regex=True, na=False)
        elif mtype == "merchant_norm":
            mask &= df["merchant_norm"].str.contains(pat.lower(), na=False, regex=False)
        else:
            continue

        idx = df.index[mask]
        df.loc[idx, "category"] = cat
        remaining.loc[idx] = False

    return df.drop(columns=["merchant_norm"])

# ---------- vendor parsers ----------
def _read_any(p: Path) -> pd.DataFrame:
    if p.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(p)
    return pd.read_csv(p, encoding="utf-8-sig")  # default CSV

def parse_chase_generic(p: Path) -> pd.DataFrame:
    """Parse both Chase bank and Chase credit-card CSV/XLSX."""
    df = _read_any(p)

    date_col   = _coalesce(df, ["Date", "Posting Date", "Post Date", "Transaction Date"])
    desc_col   = _coalesce(df, ["Description", "Description 1", "Payee", "Name", "Details", "Merchant Name"])
    amt_col    = _coalesce(df, ["Amount", "Amount (USD)", "Amount USD"])  # CC exports typically have Amount
    debit_col  = _coalesce(df, ["Debit", "Withdrawal", "Withdrawals"])    # bank-only sometimes
    credit_col = _coalesce(df, ["Credit", "Deposit", "Deposits"])        # bank-only sometimes
    cat_col    = _coalesce(df, ["Category", "Category Name"])              # may exist in card exports
    memo_col   = _coalesce(df, ["Memo", "Notes", "Note"])                # optional

    if not date_col or not desc_col:
        raise ValueError(f"{p} missing a recognizable Date/Description column")

    # Amount logic: prefer single Amount; otherwise compute credit-debit
    if amt_col:
        amt = df[amt_col].map(_num_amount)
    else:
        debit  = df[debit_col].map(_num_amount)  if debit_col  else None
        credit = df[credit_col].map(_num_amount) if credit_col else None
        if debit is None and credit is None:
            raise ValueError(f"{p} missing Amount or (Debit/Credit) columns")
        debit  = debit.fillna(0) if debit is not None else 0
        credit = credit.fillna(0) if credit is not None else 0
        amt = credit - debit  # income > 0, spend < 0

    out = pd.DataFrame({
        "date": pd.to_datetime(df[date_col], errors="coerce").dt.date,
        "description": df[desc_col].astype(str).str.strip(),
        "amount": pd.to_numeric(amt, errors="coerce"),
        "category": df[cat_col].astype(str).str.strip() if cat_col else None,
        "memo": df[memo_col].astype(str).str.strip() if memo_col else None,
        "tags": None,
    })
    out = out[~out["date"].isna()]

    vendor, route = _vendor_route_from_path(p)
    account_id = _account_id_from(vendor, route)
    out.insert(1, "account_id", account_id)
    out["amount"] = out["amount"].fillna(0).round(2)

    return out[["date","account_id","amount","description","category","memo","tags"]]

PARSERS: Dict[str, callable] = {
    "chase": parse_chase_generic,
    # add more vendors later (amex, fidelity, etc.)
}

def pick_parser(p: Path):
    vendor, _ = _vendor_route_from_path(p)
    return PARSERS.get(vendor, parse_chase_generic)

# ---------- main ----------
def _load_existing_dupe_keys(vendor: str, route: str) -> set[str]:
    """Scan existing normalized files for vendor/route and build dupe-key set."""
    root = NORM_ROOT / vendor / route.replace("/", os.sep)
    if not root.exists():
        return set()
    keys: set[str] = set()
    for f in glob.glob(str(root / "*.csv")):
        try:
            df = pd.read_csv(f, dtype={"amount": float})
            if {"date","account_id","amount","description"} <= set(df.columns):
                # rebuild keys exactly as we compute them for new rows
                desc_norm = df["description"].fillna("").map(_normalize_merchant)
                kk = (df["date"].astype(str) + "|" +
                      df["account_id"].astype(str) + "|" +
                      pd.to_numeric(df["amount"], errors="coerce").round(2).astype(str) + "|" +
                      desc_norm)
                keys.update(kk.tolist())
        except Exception:
            continue
    return keys

def normalize_all() -> None:
    # pick both CSV and Excel sources
    files = glob.glob(str(RAW_ROOT / "**" / "*-transactions.csv"), recursive=True)
    files += glob.glob(str(RAW_ROOT / "**" / "*-transactions.xlsx"), recursive=True)
    if not files:
        files = glob.glob(str(RAW_ROOT / "**" / "*.csv"), recursive=True)
    if not files:
        print(f"No raw transaction files found under {RAW_ROOT}"); return

    rules = _load_category_rules()

    # group by vendor/route so we can dedupe against existing per route
    files = [Path(f) for f in files]
    by_route: Dict[tuple[str,str], list[Path]] = {}
    for fp in files:
        v, r = _vendor_route_from_path(fp)
        by_route.setdefault((v,r), []).append(fp)

    for (vendor, route), group in sorted(by_route.items()):
        existing_keys = _load_existing_dupe_keys(vendor, route)
        frames = []
        for fp in group:
            df = pick_parser(fp)(fp)
            if df.empty:
                print(f"[SKIP] {fp} -> no rows after parsing")
                continue
            frames.append(df)

        if not frames:
            continue

        df = pd.concat(frames, ignore_index=True)

        # intra-batch dedupe
        batch_keys = _dupe_key_series(df)
        duped_mask = batch_keys.duplicated(keep="first")
        if duped_mask.any():
            df = df[~duped_mask]

        # cross-batch dedupe against normalized outputs
        cross_mask = batch_keys.isin(existing_keys)
        if cross_mask.any():
            df = df[~cross_mask]

        if df.empty:
            print(f"[normalize/tx] {vendor}/{route}: nothing new after de-dupe")
            continue

        # classify → CATEGORY ONLY
        df = _apply_category_rules(df, rules)
        classified = int(df["category"].notna().sum())
        print(f"[normalize/tx] {vendor}/{route}: classified {classified}/{len(df)} rows (after de-dupe)")

        # output
        dt_min = df["date"].min()
        dt_max = df["date"].max()
        nested_dir = NORM_ROOT / vendor / route.replace("/", os.sep)
        nested_dir.mkdir(parents=True, exist_ok=True)
        nested_path = nested_dir / f"transactions_{dt_min}_to_{dt_max}.csv"
        df.to_csv(nested_path, index=False)
        print(f"Wrote {nested_path}  rows={len(df)}")

if __name__ == "__main__":
    normalize_all()
