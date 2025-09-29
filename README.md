# Finance Dashboard

This project builds a personal finance dashboard powered by DuckDB, Python, and Power BI.

## Repository Structure

```
finance-dashboard/
├── src/
│   └── etl/
│       ├── init_db.py         # Initialize DuckDB with base schema
│       ├── load_rules.py      # Load rule/lookup tables (account_dim, category_dim, etc.)
│       ├── build_rollups.py   # Build aggregated rollups
│
├── rules/                     # CSVs for category_dim, account_dim, etc.
├── data/                      # Local DuckDB file and inputs
└── README.md
```

## Setup

1. Clone the repo and open in VS Code.
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Ensure `.env` is configured with paths:
   ```env
   DATA_DIR=C:/Users/<you>/OneDrive/FinanceData
   DUCKDB_PATH=${DATA_DIR}/finance.duckdb
   RULES_DIR=rules
   ```

## Usage

### Initialize database
```powershell
python src/etl/init_db.py
```

### Load rules/lookup tables
```powershell
python src/etl/load_rules.py
```

### Build rollups
```powershell
python src/etl/build_rollups.py
```

Or run all loaders with:
```powershell
./run_loaders.ps1
```

## Notes

- `category_dim` no longer includes `subcategory`. It uses:
  - `category`
  - `parent_category`
  - `top_bucket`
  - `notes`
  - `exclude_from_budget`
  - `is_transfer`
- Rollups and queries should join on `category` (and optionally `parent_category`) only.
- Plan to display:
  - Average yield by account (track dividend drag in taxable accounts)
  - Bond tax status (treasury/muni/taxable)
  - Qualified vs ordinary dividends
  - Average expense ratio metrics

## Next Steps

- Write CSV loader for transactions
- Build monthly cash-flow rollup/export for Power BI
- Save Power BI report as `.pbip`
