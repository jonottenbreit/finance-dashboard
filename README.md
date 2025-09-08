# Finance Dashboard

Custom personal finance dashboard for my family.

## Stack
- Python (ETL) + DuckDB (compute/storage)
- Parquet snapshots for Power BI & Excel
- Power BI Project (.pbip) versioned in Git (text diffs)
- Data lives in OneDrive; code lives in GitHub

## Paths
- Repo: C:\Users\jo136\Projects\finance-dashboard
- Data dir (from .env): C:\Users\jo136\OneDrive\FinanceData

## Repo layout
src/etl/         # ETL scripts (init_db, load_csv, build_rollups, migrations, etc.)
rules/           # category_dim.csv, budget_monthly.csv, subscriptions.csv (when added)
powerbi/         # Power BI project folder(s) (.pbip)
docs/            # decisions, next_steps, scripts overview

## Conventions
- Signs: Income > 0, Spend < 0 (display spend as positive in visuals).
- Identity: transactions.txn_id is unique (stable hash of date, account_id, normalized merchant, amount_cents, dup_seq).
- No raw data in Git: .env, .venv/, *.duckdb, and anything under OneDrive data dir are ignored.
- Transfers (cc payments/internal moves) are excluded from P&L (tagging step coming).
- Subscriptions can be flagged via simple rules in rules/subscriptions.csv.

## Quick start (local)
PowerShell:
    .\.venv\Scripts\Activate
    python src\etl\init_db.py
    python src\etl\migrate_001_add_unique_txn_id.py   # safe to re-run
    python src\etl\load_csv.py
    python src\etl\build_rollups.py
Optional:
    python src\etl\migrate_002_subscriptions.py
    python src\etl\load_subscription_rules.py
    python src\etl\export_for_excel.py

## Power BI
- Get Data → Parquet → C:\Users\jo136\OneDrive\FinanceData\exports\monthly_cashflow.parquet
- Build visuals (e.g., line chart: month vs income/spending/net).
- Save As → Power BI project (.pbip) to: powerbi\FinanceDashboard\
- Commit the entire powerbi\FinanceDashboard\ folder.

## What’s already done
- .env points to OneDrive data dir
- finance.duckdb created with transactions table
- Unique index on transactions(txn_id) (migration 001)
- CSV loader normalizes data and inserts idempotently
- monthly_cashflow rollup exported to exports\monthly_cashflow.parquet
- Power BI project saved locally (.pbip)

## Next steps
- Add more sample rows across months; rerun loader + rollups; refresh PBI
- Seed rules\category_dim.csv and rules\budget_monthly.csv; add a Budget vs Actual matrix
- Add balance_snapshot + a Net Worth line (export monthly_net_worth.parquet)
- Add rules\subscriptions.csv and run subscription tagging; add slicer in PBI

## Docs
- Scripts overview: docs/scripts.md
- What’s next: docs/next_steps.md

## Troubleshooting
- Parquet shows only one month → add more sample txns → run load_csv.py + build_rollups.py → Refresh in PBI.
- .env not read → run from repo root with venv active; ensure scripts print the same DB path.
- ON CONFLICT error → run migrate_001_add_unique_txn_id.py (unique index on txn_id).
