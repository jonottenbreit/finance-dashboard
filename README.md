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
src/etl/         # ETL scripts (init_db, load_csv, load_categories_and_budget, build_rollups, migrations, etc.)
rules/           # category_dim.csv, budget_monthly.csv, subscriptions.csv (when added)
powerbi/         # Power BI project folder(s) (.pbip)
docs/            # decisions, next_steps, scripts overview
```

## Conventions
- **Signs:** Income > 0, Spend < 0 (display spend as positive in visuals).
- **Identity:** `transactions.txn_id` is unique (stable hash of date, account_id, normalized merchant, amount_cents, dup_seq).
- **No raw data in Git:** `.env`, `.venv/`, `*.duckdb`, and anything under the OneDrive data dir are ignored.
- **Transfers** (cc payments/internal moves) are excluded from P&L (tagging step coming).
- **Subscriptions** can be flagged via simple rules in `rules/subscriptions.csv`.

---

## Reporting exports (Parquet)
The ETL writes these to `C:\Users\jo136\OneDrive\FinanceData\exports\`:

- `monthly_cashflow.parquet`
- `budget_monthly.parquet`
- `category_dim.parquet`
- `monthly_actuals_by_category.parquet`
- `month_dim.parquet`

Regenerate with:
```powershell
.\.venv\Scripts\Activate
python src\etl\build_rollups.py
```

## Power BI model (relationships)
Load all Parquet files above, then create **single-direction, 1→*** relationships:

- `month_dim[month]` → `budget_monthly[month]`
- `month_dim[month]` → `monthly_actuals_by_category[month]`
- `month_dim[month]` → `monthly_cashflow[month]`
- `category_dim[category]` → `budget_monthly[category]`
- `category_dim[category]` → `monthly_actuals_by_category[category]`

Optional:
- Table tools → **Mark `month_dim` as Date table**.
- Hide the `month` columns on fact tables (use `month_dim[month]` in visuals).

## Measures (lives under `Measures_Main`)
Create these measures under your **Measures_Main** table. Use Display Folders (e.g., *Budget vs Actual*, *Cashflow*) if you like.

```DAX
-- Budget vs Actual
Budget Amount :=
SUM(budget_monthly[amount])

Actual Amount :=
VAR amt = SUM(monthly_actuals_by_category[actual_signed])
RETURN IF(SELECTEDVALUE(category_dim[top_bucket]) = "Income", amt, -amt)

Variance := [Actual Amount] - [Budget Amount]
Variance % := DIVIDE([Variance], [Budget Amount])

-- Cashflow
CF Income := SUM(monthly_cashflow[income])
CF Spending := -SUM(monthly_cashflow[spending])
CF Net := SUM(monthly_cashflow[net_cashflow])
```

---

## Budget & categories
- Definitions live in repo **`rules/`**:
  - `category_dim.csv` → `category,parent_category,top_bucket`
  - `budget_monthly.csv` → `month,category,amount` (amounts **positive** for income & spend)
- Load into DuckDB:
```powershell
python src\etl\load_categories_and_budget.py
python src\etl\build_rollups.py
```

## Quick start (local)
```powershell
cd C:\Users\jo136\Projects\finance-dashboard
.\.venv\Scripts\Activate
python src\etl\init_db.py
python src\etl\migrate_001_add_unique_txn_id.py   # safe to re-run
python src\etl\load_csv.py
python src\etl\load_categories_and_budget.py
python src\etl\build_rollups.py
# then Refresh in Power BI
```

## What’s already done
- `.env` points to OneDrive data dir
- `finance.duckdb` created with `transactions` table
- Unique index on `transactions(txn_id)` (migration 001)
- CSV loader normalizes data and inserts idempotently
- Rollups exported to Parquet (cashflow, budget, categories, actuals-by-category, month_dim)
- Power BI project saved locally (.pbip)

## Troubleshooting
- **Parquet shows only one month** → add more sample txns → run `load_csv.py` + `build_rollups.py` → Refresh in PBI.
- **“.env not read”** → run from repo root with venv active; ensure scripts print the expected DB path.
- **Unknown/null categories** → add missing names to `rules/category_dim.csv` → reload loaders → rebuild rollups.
- **ON CONFLICT error** → run `migrate_001_add_unique_txn_id.py` (unique index on `txn_id`).

## Docs
- Scripts overview: `docs/scripts.md`
- What’s next: `docs/next_steps.md`
