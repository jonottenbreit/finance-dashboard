# Finance Dashboard

This project builds a comprehensive personal finance and retirement dashboard powered by **DuckDB**, **Python**, and **Power BI**.

---

## Repository Structure
```
finance-dashboard/
├── src/
│   └── etl/
│       ├── init_db.py          # Initialize DuckDB with base schema
│       ├── load_rules.py       # Load rule/lookup tables (account_dim, category_dim, etc.)
│       ├── load_positions.py   # Load brokerage/investment positions
│       ├── normalize_positions.py # Normalize institution exports
│       ├── build_rollups.py    # Build aggregated rollups for Power BI
│       ├── load_retirement.py  # Build retirement inflow/outflow projections
│
├── rules/                      # Lookup tables (account_dim, category_dim, security_dim)
├── data/                       # DuckDB file and exports
└── README.md
```

---

## Setup

1. **Clone the repo** and open in VS Code.
2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   ```
3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Ensure `.env` includes paths:**
   ```env
   DATA_DIR=C:/Users/<you>/OneDrive/FinanceData
   DUCKDB_PATH=${DATA_DIR}/finance.duckdb
   RULES_DIR=rules
   ```

---

## Usage

### Initialize Database
```powershell
python src/etl/init_db.py
```

### Load Rules / Lookup Tables
```powershell
python src/etl/load_rules.py
```

### Load and Normalize Positions
```powershell
python src/etl/load_positions.py
```

### Build Rollups (for Power BI)
```powershell
python src/etl/build_rollups.py
```

### Build Retirement Projections
```powershell
python src/etl/load_retirement.py
```

Or run everything:
```powershell
./run_loaders.ps1
```

---

## Data Flow Summary

| Step | File | Purpose |
|------|------|----------|
| 1 | **init_db.py** | Creates initial schema for DuckDB database. |
| 2 | **load_rules.py** | Loads static lookup tables (account_dim, category_dim, etc.). |
| 3 | **load_positions.py / normalize_positions.py** | Imports and standardizes brokerage and manual positions. |
| 4 | **build_rollups.py** | Aggregates transactions, budgets, and balances for Power BI. |
| 5 | **load_retirement.py** | Loads and inflates future inflows/outflows from `retirement_assumptions.csv`, applies real and nominal return logic, and exports `ret_inflows.csv` and `ret_outflows.csv`. |

---

## Key File Behavior

- **Manual Positions**  
  Saved under `FinanceData/positions/normalized/manual/positions_<YYYY-MM-DD>.csv`.  
  To update: duplicate and update the date for the latest snapshot.

- **Budgets**  
  Lives only as `budgets.csv` (not in DuckDB). If the current month is blank, the prior month’s data is copied forward.

- **Retirement Assumptions**  
  Stored at `FinanceData/retirement/retirement_assumptions.csv`.  
  Defines base assumptions for inflation, returns, spending, and contribution timing.  
  The loader expands these over time and saves `ret_inflows.csv` and `ret_outflows.csv`.

---

## Power BI Integration

### Data Sources
- **DuckDB (positions, transactions, rollups)** — actuals
- **ret_inflows.csv / ret_outflows.csv** — projected cashflows
- **ret_starting_balances.csv** — current portfolio baseline

### Model Relationships
```
YearTable[Year] 1 → * Cashflows[Year]
Cashflows joined to ret_starting_balances via account_id (optional)
```

### Core Measures
```DAX
-- Clean type normalization
TypeNorm =
VAR t = UPPER(TRIM(Cashflows[Type]))
RETURN SWITCH(TRUE(),
    t = "INFLOW" || t = "INFLOWS", "INFLOW",
    t = "OUTFLOW" || t = "OUTFLOWS", "OUTFLOW",
    BLANK()
)

-- Core flows
Net Flow =
VAR Inflow  = CALCULATE(SUM(Cashflows[Value]), Cashflows[TypeNorm] = "INFLOW")
VAR Outflow = CALCULATE(SUM(Cashflows[Value]), Cashflows[TypeNorm] = "OUTFLOW")
RETURN COALESCE(Inflow,0) - COALESCE(Outflow,0)

Cumulative Net Flow =
VAR Y = MAX(YearTable[Year])
RETURN CALCULATE([Net Flow], FILTER(ALLSELECTED(YearTable[Year]), YearTable[Year] <= Y))

-- Nominal return logic from globals
Nominal Return Rate :=
VAR r = COALESCE([Real Return Rate], 0)
VAR i = COALESCE([Inflation Rate], 0)
RETURN (1 + r) * (1 + i) - 1

-- Portfolio growth-aware net worth
Net Worth (with growth) =
VAR y = MAX(YearTable[Year])
VAR r = COALESCE([Nominal Return Rate], 0)
VAR base = [Base Year]
VAR StartCompounded = [Start Balance] * POWER(1 + r, y - base)
VAR FlowsCompounded =
    SUMX(
        FILTER(Cashflows, Cashflows[Year] <= y),
        VAR sign = IF(Cashflows[TypeNorm] = "INFLOW", 1, -1)
        RETURN sign * Cashflows[Value] * POWER(1 + r, y - Cashflows[Year])
    )
RETURN StartCompounded + FlowsCompounded
```

### Visuals
- **Line Chart:** Net Worth (with growth) by Year
- **Clustered Columns:** Net Flow and Investment Return by Year
- **Table:** Cashflows by category and year

---

## Recent Enhancements

- `load_retirement.py` now inflates withdrawals and social security flows using CPI and applies nominal growth logic.
- Added `real_return_rate` and `inflation_rate` assumptions to `ret_globals` for downstream DAX measures.
- Implemented `manual_positions.csv` ingestion for assets not tied to institutions (TIPS, HSA, etc.).
- Added cumulative return logic in Power BI for dynamic portfolio projection.

---

## Next Steps
- Merge real portfolio performance (DuckDB) with projected cashflow forecast into one Power BI view.
- Add toggles for *nominal vs real* view and *scenario-based returns* (e.g., 4%, 6%, 8%).
- Introduce withdrawal sequencing and tax-bucket logic (Taxable / Roth / Traditional).
- Extend Python loader to project required minimum distributions (RMDs) and Social Security tax treatment.

