# -*- coding: utf-8 -*-
r"""
Normalize raw positions into canonical CSV:
  as_of_date, account_id, symbol, shares, price, market_value
Outputs mirror raw path: positions/normalized/<vendor>/<route>/positions_<date>.csv
"""
import os, re, glob
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DATA_DIR  = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
RAW_ROOT  = DATA_DIR / "positions" / "raw"
NORM_ROOT = DATA_DIR / "positions" / "normalized"
NORM_ROOT.mkdir(parents=True, exist_ok=True)

# Map a raw path (vendor, route) to an explicit account_id from rules/account_dim.csv
# You asked to set Chase brokerage_taxable -> BRK_JOINT.
ACCOUNT_ID_OVERRIDES = {
    ("chase", "brokerage_taxable"): "BRK_JOINT",
    ("chase", "IRA_JON"): "IRA_JON",
    ("chase", "IRA_JON_ROTH"): "IRA_JON_ROTH",
    ("chase", "IRA_SHANNA"): "IRA_SHANNA",
    ("alight", "all_accounts"): "401K_JON",
    # add more like: ("fidelity", "roth"): "ROTH_JON"
}

def _num(x):
    if pd.isna(x): return None
    s = str(x).strip().replace(",", "").replace("$", "")
    # parentheses = negative
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").strip()
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None

def _infer_date_from_name(p: Path):
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", p.name)
    return pd.to_datetime("-".join(m.groups())).date() if m else None

def _slug_symbol_from_name(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)              # spaces -> underscore
    s = re.sub(r"[^A-Za-z0-9_]+", "", s)    # drop non-alnum/underscore
    s = re.sub(r"_+", "_", s).strip("_")
    return s.upper() if s else "UNKNOWN_FUND"

def _vendor_route_from_path(p: Path):
    # Expect .../raw/<vendor>/<route>/file.csv ; route may be nested (join with '/')
    parts = [s.lower() for s in p.parts]
    try:
        i = parts.index("raw")
        vendor = parts[i+1] if i+1 < len(parts)-1 else "unknown"
        route  = "/".join(parts[i+2:-1]) or "unknown"
        return vendor, route
    except ValueError:
        return "unknown", "unknown"

def _account_id_from(vendor: str, route: str):
    key = (vendor.lower(), route.lower())
    if key in ACCOUNT_ID_OVERRIDES:
        return ACCOUNT_ID_OVERRIDES[key]
    # If the route itself looks like an account_id, use it
    import re
    cand = route.replace("/", "_").upper()
    if re.fullmatch(r"[A-Z0-9_]+", cand):
        return cand
    # fallback: vendor_route
    return (vendor + "_" + route.replace("/", "_")).upper()

def parse_chase_positions(p: Path) -> pd.DataFrame:
    """Parse Chase brokerage positions export; trims footnotes."""
    df = pd.read_csv(p, encoding="utf-8-sig")
    # Trim at 'FOOTNOTES' sentinel if present
    if "Asset Class" in df.columns:
        idx = df.index[df["Asset Class"].astype(str).str.strip().str.upper() == "FOOTNOTES"]
        if len(idx) > 0:
            df = df.loc[:idx[0]-1]

    for c in ("Quantity", "Price", "Value"):
        if c in df.columns:
            df[c] = df[c].map(_num)

    as_of = None
    if "As of" in df.columns and df["As of"].notna().any():
        try: as_of = pd.to_datetime(df["As of"].dropna().astype(str).iloc[0]).date()
        except Exception: pass
    if as_of is None:
        as_of = _infer_date_from_name(p)

    vendor, route = _vendor_route_from_path(p)
    account_id = _account_id_from(vendor, route)

    keep = (
        (df.get("Ticker").notna()) |
        (df.get("Quantity").fillna(0) != 0) |
        (df.get("Value").fillna(0) != 0) |
        (df.get("Description").astype(str).str.contains("SWEEP|CASH", case=False, na=False))
    )
    pos = df.loc[keep].copy()

    out = pd.DataFrame({
        "as_of_date": as_of,
        "account_id": account_id,
        "symbol": pos.get("Ticker").fillna("CASH").astype(str).str.strip(),
        "shares": pos.get("Quantity"),
        "price": pos.get("Price"),
        "market_value": pos.get("Value"),
        "description": pos.get("Description"),
        "vendor": vendor,
        "route": route,
    })

    # fill market_value if missing
    mv_missing = out["market_value"].isna()
    out.loc[mv_missing, "market_value"] = out.loc[mv_missing, "shares"] * out.loc[mv_missing, "price"]

    # normalize CASH
    is_cash = (out["symbol"].eq("CASH")) | (out["description"].astype(str).str.contains("SWEEP|CASH", case=False, na=False))
    out.loc[is_cash & out["market_value"].notna(), "symbol"] = "CASH"
    out.loc[is_cash & out["market_value"].notna(), "price"] = 1.0
    out.loc[is_cash & out["market_value"].notna(), "shares"] = out.loc[is_cash, "market_value"]

    return out[["as_of_date","account_id","symbol","shares","price","market_value","vendor","route"]]

def parse_alight_positions(p: Path) -> pd.DataFrame:
    """
    Parse Alight 401k positions. Uses 'Fund Name' (slugged) as symbol.
    Recognizes Closing Balance and other common headers.
    """
    df = pd.read_csv(p, encoding="utf-8-sig")
    # normalize column names into a search-friendly form
    def norm(s): return re.sub(r"[^a-z0-9]", "", s.lower())

    cols_norm = {c: norm(c) for c in df.columns}

    def find_col(candidates, required=False):
        # candidates = list of tokens to look for in normalized col names, in priority order
        for token in candidates:
            for col, n in cols_norm.items():
                if token in n:        # substring match (tolerant of variants)
                    return col
        if required:
            raise ValueError(f"{p} missing required column like: {candidates}")
        return None

    name_col  = find_col(["fundname","investmentoption","name"], required=True)
    # units/price are optional in many Alight exports
    units_col = find_col(["unitsheld","units","unit","quantity","shares"])
    price_col = find_col(["unitprice","priceperunit","unitvalue","nav","price"])
    # IMPORTANT: include 'closingbalance' first
    value_col = find_col([
        "closingbalance","endingbalance","currentbalance",
        "marketvalue","currentvalue","value","balance"
    ], required=False)

    # parse numerics
    for c in [units_col, price_col, value_col]:
        if c:
            df[c] = df[c].map(_num)

    # as-of date
    asof_col = find_col(["asof","asofthe","effective"], required=False)
    as_of = None
    if asof_col and df[asof_col].notna().any():
        try:
            as_of = pd.to_datetime(df[asof_col].dropna().astype(str).iloc[0]).date()
        except Exception:
            as_of = None
    if as_of is None:
        as_of = _infer_date_from_name(p)

    vendor, route = _vendor_route_from_path(p)
    account_id = _account_id_from(vendor, route)

    pos = df[df[name_col].notna()].copy()
    fund_name = pos[name_col].astype(str).str.strip()
    symbol = fund_name.apply(_slug_symbol_from_name)

    shares = pos[units_col] if units_col in pos.columns else None
    price  = pos[price_col] if price_col in pos.columns else None
    value  = pos[value_col] if value_col in pos.columns else None

    out = pd.DataFrame({
        "as_of_date": as_of,
        "account_id": account_id,
        "symbol": symbol,
        "shares": shares,
        "price": price,
        "market_value": value,
        "vendor": vendor,
        "route": route,
    })

    # If value missing but shares*price available, compute it
    mv_missing = out["market_value"].isna()
    out.loc[mv_missing & out["shares"].notna() & out["price"].notna(), "market_value"] = \
        out.loc[mv_missing, "shares"] * out.loc[mv_missing, "price"]

    # If there are unitless balances (value present, shares missing), treat price=1
    unitless = out["market_value"].notna() & out["shares"].isna()
    out.loc[unitless, "shares"] = out.loc[unitless, "market_value"]
    out.loc[unitless, "price"]  = 1.0

    # final dtypes
    out["shares"] = pd.to_numeric(out["shares"], errors="coerce")
    out["price"]  = pd.to_numeric(out["price"], errors="coerce")
    out["market_value"] = pd.to_numeric(out["market_value"], errors="coerce")

    # (optional) quick debug so you can see what columns were chosen
    print(f"[normalize/alight] file={p.name} name={name_col} units={units_col} "
          f"price={price_col} value={value_col} as_of={as_of}")

    return out[["as_of_date","account_id","symbol","shares","price","market_value","vendor","route"]]

# Register parsers here; more vendors later
PARSERS = {
    "chase":  parse_chase_positions,
    "alight": parse_alight_positions,
}

def pick_parser(p: Path):
    vendor, _ = _vendor_route_from_path(p)
    return PARSERS.get(vendor, parse_chase_positions)

def normalize_all():
    files = glob.glob(str(RAW_ROOT / "**" / "*-positions.csv"), recursive=True)
    if not files:
        print("No raw position files found."); return

    all_rows = []
    for f in files:
        fp = Path(f)
        df = pick_parser(fp)(fp)
        all_rows.append(df)

    df_all = pd.concat(all_rows, ignore_index=True)

    # Collapse to symbol-level per (date, account, vendor, route)
    grp = df_all.groupby(
        ["as_of_date","account_id","vendor","route","symbol"],
        dropna=False,
        as_index=False
    ).agg(
        shares=("shares","sum"),
        market_value=("market_value","sum")
    )
    grp["price"] = (grp["market_value"] / grp["shares"]).where(grp["shares"] > 0)

    # Write one canonical file per (date, vendor, route)
    for (adate, vendor, route), df_part in grp.groupby(["as_of_date","vendor","route"]):
        out_dir = NORM_ROOT / vendor / route.replace("/", os.sep)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"positions_{adate}.csv"
        df_part[["as_of_date","account_id","symbol","shares","price","market_value"]].to_csv(out_path, index=False)
        print(f"Wrote {out_path} rows={len(df_part)}")

if __name__ == "__main__":
    normalize_all()
