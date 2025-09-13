# Finance Dashboard

Custom personal finance dashboard for my family.

## Stack
- Python (ETL) + DuckDB (compute/storage)
- Parquet snapshots for Power BI & Excel
- Power BI Project (.pbip) versioned in Git (text diffs)
- Data lives in OneDrive; code lives in GitHub

## Paths
- Repo: `C:\Users\jo136\Projects\finance-dashboard`
- Data dir (from `.env`): `C:\Users\jo136\OneDrive\FinanceData`

## Repo layout
```
src/etl/         # ETL scripts (init_db, load_csv, load_categories_and_budget, load_accounts_and_balances, build_rollups, ...)
rules/           # category_dim.csv, budget_monthly.csv, account_dim.csv, (optional) security_dim.csv, target_allocation.csv
powerbi/         # Power BI project(s) (.pbip)
docs/            # decisions, next_steps, scripts overview
```

## Conventions
- **Signs:** Income > 0, Spend < 0 (display spend as positive in visuals).
- **Identity:** `transactions.txn_id` is unique (stable hash of date, account_id, normalized merchant, amount_cents, dup_seq).
- **No raw data in Git:** `.env`, `.venv/`, `*.duckdb`, and anything under the OneDrive data dir are ignored.
- **Transfers** (cc payments/internal moves) are excluded from P&L (tagging step coming).
- **Balances:** liabilities may be typed positive in CSVs; loader normalizes them to **negative** using `account_dim.type`.
- **Net worth in visuals:** do **not** sum across months; use end-of-period measures (see below).

---

## Reporting exports (Parquet)
Written to `C:\Users\jo136\OneDrive\FinanceData\exports\`:

- **P&L**
  - `monthly_cashflow.parquet`
  - `monthly_actuals_by_category.parquet`
  - `budget_monthly.parquet`
  - `category_dim.parquet`
  - `month_dim.parquet`
- **Net worth**
  - `monthly_net_worth.parquet`
  - `monthly_net_worth_by_group.parquet`
- **(Optional, when positions are added)**
  - `monthly_allocation.parquet`
  - `allocation_vs_target.parquet`

Regenerate:
```powershell
.\.venv\Scripts\Activate
python src\etl\build_rollups.py
```

## Power BI model (relationships)
Create **single-direction, 1→*** relationships:

- `month_dim[month]` → `budget_monthly[month]`
- `month_dim[month]` → `monthly_actuals_by_category[month]`
- `month_dim[month]` → `monthly_cashflow[month]`
- `month_dim[month]` → `monthly_net_worth[month]`
- `month_dim[month]` → `monthly_net_worth_by_group[month]`
- `category_dim[category]` → `budget_monthly[category]`
- `category_dim[category]` → `monthly_actuals_by_category[category]`

Optional:
- Table tools → **Mark `month_dim` as Date table**.
- Hide the `month` columns on fact tables (use `month_dim[month]` in visuals).

## Measures (under `Measures_Main`)
Use Display Folders (e.g., *Budget vs Actual*, *Cashflow*, *Net Worth*).

```DAX
-- Budget vs Actual
Budget Amount := SUM(budget_monthly[amount])

Actual Amount :=
VAR amt = SUM(monthly_actuals_by_category[actual_signed])
RETURN IF(SELECTEDVALUE(category_dim[top_bucket]) = "Income", amt, -amt)

Variance := [Actual Amount] - [Budget Amount]
Variance % := DIVIDE([Variance], [Budget Amount])

-- Cashflow
CF Income := SUM(monthly_cashflow[income])
CF Spending := -SUM(monthly_cashflow[spending])
CF Net := SUM(monthly_cashflow[net_cashflow])

-- Net Worth (end-of-period safe)
NW Total :=
IF(
  HASONEVALUE('month_dim'[month]),
  SUM(monthly_net_worth[net_worth]),
  CALCULATE(SUM(monthly_net_worth[net_worth]), LASTDATE('month_dim'[month]))
)

NW Assets :=
IF(HASONEVALUE('month_dim'[month]),
  SUM(monthly_net_worth[assets]),
  CALCULATE(SUM(monthly_net_worth[assets]), LASTDATE('month_dim'[month]))
)

NW Liabilities :=
IF(HASONEVALUE('month_dim'[month]),
  SUM(monthly_net_worth[liabilities]),
  CALCULATE(SUM(monthly_net_worth[liabilities]), LASTDATE('month_dim'[month]))
)

NW Liquid :=
IF(HASONEVALUE('month_dim'[month]),
  SUM(monthly_net_worth[liquid_net_worth]),
  CALCULATE(SUM(monthly_net_worth[liquid_net_worth]), LASTDATE('month_dim'[month]))
)

NW Investable :=
IF(HASONEVALUE('month_dim'[month]),
  SUM(monthly_net_worth[investable_assets]),
  CALCULATE(SUM(monthly_net_worth[investable_assets]), LASTDATE('month_dim'[month]))
)

NW By Group (EOP) :=
VAR LatestMonth = MAX('month_dim'[month])
RETURN CALCULATE(SUM(monthly_net_worth_by_group[value]), 'month_dim'[month] = LatestMonth)

NW Group % := DIVIDE([NW By Group (EOP)], [NW Total])
```

---

## Data inputs (CSVs)

### Categories & budget (repo/rules)
- `rules/category_dim.csv` → `category,parent_category,top_bucket`
- `rules/budget_monthly.csv` → `month,category,amount` (amounts **positive** for income & spend)

### Accounts & balances
- `rules/account_dim.csv` → `account_id,account_name,owner,type,acct_group,tax_bucket,liquidity,include_networth,include_liquid`
- `C:\Users\jo136\OneDrive\FinanceData\balances\*.csv` → `as_of_date,account_id,balance`

*(Optional) Holdings for allocation later:*
- `rules/security_dim.csv` (symbol → asset_class, etc.)
- `rules/target_allocation.csv` (asset_class targets summing to 1.0)
- `C:\Users\jo136\OneDrive\FinanceData\positions\*.csv` → `as_of_date,account_id,symbol,shares,price,market_value`

## Quick start (local)
```powershell
cd C:\Users\jo136\Projects\finance-dashboard
.\.venv\Scripts\Activate
python src\etl\init_db.py
python src\etl\migrate_001_add_unique_txn_id.py       # safe to re-run
python src\etl\load_csv.py                             # transactions
python src\etl\load_categories_and_budget.py           # rules/
python src\etl\load_accounts_and_balances.py           # rules/ + balances/
# (optional) python src\etl\load_positions.py          # rules/ + positions/
python src\etl\build_rollups.py                        # writes Parquet exports
# then Refresh in Power BI
```

## What’s already done
- `.env` points to OneDrive data dir
- `finance.duckdb` created with `transactions` (+ unique index on `txn_id`)
- CSV loader normalizes data and inserts idempotently
- **Net worth** rollups + exports (monthly and by group)
- Power BI project saved locally (.pbip)

## Troubleshooting
- **Unknown/null categories** → append to `rules/category_dim.csv` → reload loaders → rebuild rollups.
- **Unknown account_id in balances** → add to `rules/account_dim.csv`.
- **ON CONFLICT error** → ensure `migrate_001_add_unique_txn_id.py` was run (unique index on `txn_id`).
- **Parquet shows one month** → add more sample txns/balances → re-run `build_rollups.py` → Refresh PBI.
- **Net worth cards sum months** → use the provided EOP measures, not raw SUMs.

## Docs
- Scripts overview: `docs/scripts.md`
- What’s next: `docs/next_steps.md`
