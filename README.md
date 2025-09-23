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
src/etl/         # ETL scripts (init_db, load_rules, load_transactions, load_positions, load_accounts_and_balances, build_rollups, ...)
rules/           # category_dim.csv, budget_monthly.csv, account_dim.csv, security_dim.csv, target_allocation.csv
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
- **Allocations:** weights are derived from positions → grouped by asset_class, compared against targets.

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
- **Portfolio**
  - `monthly_allocation.parquet`
  - `allocation_vs_target.parquet`
  - `security_dim.parquet`
  - `positions_enriched.parquet`

Regenerate (in powershell):
```powershell
.\.venv\Scripts\Activate
.\run_loaders.ps1
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

Optional portfolio joins:
- `month_dim[month]` → `monthly_allocation[month]`
- `month_dim[month]` → `allocation_vs_target[month]`
- `positions_enriched[symbol]` → `security_dim[symbol]`
- `month_dim[month]` → `positions_enriched[month]`

Optional:
- Table tools → **Mark `month_dim` as Date table**.
- Hide the `month` columns on fact tables (use `month_dim[month]` in visuals).

## Measures (under `Measures_Main`)
Use Display Folders (e.g., *Budget vs Actual*, *Cashflow*, *Net Worth*, *Portfolio*).

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

For Portfolio tab:
```DAX
Actual Weight := AVERAGE(monthly_allocation[actual_weight])
Target Weight := AVERAGE(allocation_vs_target[target_weight])
Variance Weight := [Actual Weight] - [Target Weight]
```

---

## Data inputs (CSVs)

### Categories & budget (repo/rules)
- `rules/category_dim.csv` → `category,parent_category,top_bucket`
- `rules/budget_monthly.csv` → `month,category,amount` (amounts **positive** for income & spend)

### Accounts & balances
- `rules/account_dim.csv` → `account_id,account_name,owner,type,acct_group,tax_bucket,liquidity,include_networth,include_liquid`
- `C:\Users\jo136\OneDrive\FinanceData\balances\*.csv` → `as_of_date,account_id,balance`

### Holdings & allocation
- `rules/security_dim.csv` → `symbol,asset_class,region,style`
- `rules/target_allocation.csv` → `asset_class,target_weight` (sums to 1.0)
- `C:\Users\jo136\OneDrive\FinanceData\positions\*.csv` → `as_of_date,account_id,symbol,shares,price,market_value`

### Transactions
- `C:\Users\jo136\OneDrive\FinanceData\transactions\*.csv` → `date,account_id,amount,description,category,subcategory,memo`  
  (loader normalizes, computes txn_id, merchant_norm, amount_cents)

---

## Quick start (local)
```powershell
cd C:\Users\jo136\Projects\finance-dashboard
.\.venv\Scripts\Activate
python src\etl\init_db.py                         # one-time
python src\etl\load_rules.py                      # rules → dims
python src\etl\load_transactions.py               # transactions
python src\etl\load_positions.py                  # positions
python src\etl\load_accounts_and_balances.py      # balances
python src\etl\build_rollups.py                   # writes Parquet exports
# then Refresh in Power BI
```

## What’s already done
- `.env` points to OneDrive data dir
- `finance.duckdb` created with `transactions` (+ unique index on `txn_id`)
- CSV loaders normalize data and insert idempotently
- **Net worth** rollups + exports (monthly and by group)
- **Portfolio** rollups + exports (allocation vs target, positions enriched, security_dim)
- Power BI project saved locally (.pbip)

## Troubleshooting
- **Unknown/null categories** → append to `rules/category_dim.csv` → reload loaders → rebuild rollups.
- **Unknown account_id in balances** → add to `rules/account_dim.csv`.
- **ON CONFLICT error** → ensure unique index exists on `txn_id`.
- **Parquet shows one month** → add more sample txns/balances/positions → re-run loaders → rebuild rollups.
- **Net worth cards sum months** → use the provided EOP measures, not raw SUMs.
- **Allocation not showing** → confirm positions + security_dim + target_allocation are loaded.
