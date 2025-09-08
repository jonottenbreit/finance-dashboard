# Next steps — Finance Dashboard
_As of 2025-09-08_

## What’s already done
- Repo: `C:\Users\jo136\Projects\finance-dashboard` (outside OneDrive)
- Python venv: `.venv` (activate with `.\.venv\Scripts\Activate`)
- `.env` → `DATA_DIR=C:\Users\jo136\OneDrive\FinanceData`
- DuckDB created: `finance.duckdb`
- Tables: `transactions`
- Unique index migration on `transactions(txn_id)`
- CSV loader with stable `txn_id` (no memo; normalized merchant; tie-breaker)
- Rollup export: `exports\monthly_cashflow.parquet`
- Power BI Desktop project (.pbip) saved under `powerbi/FinanceDashboard`

## Immediate next bites (in order)
1) **Expand sample data** (optional, for nicer charts)
   - Append a few more transactions across multiple months to `sample_transactions.csv`
   - Run: `python src\etl\load_csv.py` → `python src\etl\build_rollups.py` → refresh PBI

2) **Seed categories + budget**
   - Create two CSVs in `rules/`:
     - `category_dim.csv` (see templates below)
     - `budget_monthly.csv` (3–6 months; a few categories)
   - Write a tiny loader `src/etl/load_categories_and_budget.py` to load/replace those into DuckDB
   - In PBI, add a **Matrix**: Month × Category with measures: Actual, Budget, Variance, Variance %

3) **Add balances (Net Worth)**
   - New table `balance_snapshot(as_of_date, account_id, account, type, balance)`
   - Create a small CSV `rules/balances_sample.csv` → loader `src/etl/load_balances.py`
   - Build `monthly_net_worth.parquet` export
   - In PBI, add a **Net Worth** line chart + bars for Liquid vs Retirement

4) **Subscriptions breakout**
   - Already have migrations + loader scaffolding
   - Create `rules/subscriptions.csv` (Spotify/Netflix/etc.) and run `python src\etl\load_subscription_rules.py`
   - In PBI, add a **Slicer** on `is_subscription` and a table by `subscription_dim[name]`

5) **Excel tap-out (optional)**
   - Run `python src\etl\export_for_excel.py`
   - Share `FinanceData\exports\excel\finance_preview.xlsx` with spouse via OneDrive

6) **Automate daily refresh (when ready)**
   - Batch script `ops\tasks\run_etl.bat` to run:
     ```
     call .\.venv\Scripts\Activate
     python src\etl\init_db.py
     python src\etl\load_csv.py
     python src\etl\load_categories_and_budget.py
     python src\etl\load_balances.py
     python src\etl\load_subscription_rules.py
     python src\etl\build_rollups.py
     python src\etl\export_for_excel.py
     ```
   - Windows Task Scheduler → daily 6:00 AM

## Templates (drop these in `rules/`)

### `category_dim.csv`
