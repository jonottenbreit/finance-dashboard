# Finance Dashboard

This project builds a comprehensive personal finance and retirement dashboard powered by **DuckDB**, **Python**, and **Power BI**.

---

## Repository Structure
```
finance-dashboard/
├── src/
│   └── etl/
│       ├── init_db.py              # Initialize DuckDB with base schema
│       ├── load_rules.py           # Load rule/lookup tables (account_dim, category_dim, etc.)
│       ├── load_positions.py       # Load brokerage/investment positions
│       ├── normalize_positions.py  # Normalize institution exports
│       ├── build_rollups.py        # Build aggregated rollups for Power BI
│       ├── load_retirement.py      # Build retirement inflow/outflow projections
│       ├── run_sql.py              # Utility to run ad-hoc SQL from the CLI
│
├── rules/                          # Lookup tables (account_dim, category_dim, security_dim)
├── data/                           # DuckDB file and exports
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

### Run Ad‑Hoc SQL
```powershell
python .\src\etl\run_sql.py "SELECT ..."
```

Or run everything:
```powershell
./run_loaders.ps1
```

---

## Data Flow Summary

| Step | File | Purpose |
|------|------|---------|
| 1 | **init_db.py** | Creates initial schema for DuckDB database. |
| 2 | **load_rules.py** | Loads static lookup tables (account_dim, category_dim, security_dim, globals). |
| 3 | **load_positions.py / normalize_positions.py** | Imports and standardizes brokerage and manual positions. |
| 4 | **build_rollups.py** | Aggregates transactions, budgets, and balances for Power BI. |
| 5 | **load_retirement.py** | Expands `retirement_assumptions.csv` into yearly **inflows/outflows**, applies **real→nominal** conversion, grows balances, and exports `ret_inflows.csv`, `ret_outflows.csv`, and `ret_starting_balances.csv`. |

---

## Power BI Integration

### Data Sources
- **DuckDB** (positions, transactions, rollups) — actuals  
- **ret_inflows.csv / ret_outflows.csv** — projected cashflows  
- **ret_starting_balances.csv** — current portfolio baseline

### Model Relationships
```
YearTable[Year] 1 → * Cashflows[Year]
Cashflows joined to ret_starting_balances via account_id (optional)
```

### Core Measures (DAX)
> Normalize cashflow types, compute flows, nominal returns, and growth-aware net worth.

```DAX
-- Clean type normalization
TypeNorm =
VAR t = UPPER(TRIM(Cashflows[Type]))
RETURN
    SWITCH(
        TRUE(),
        t = "INFLOW" || t = "INFLOWS", "INFLOW",
        t = "OUTFLOW" || t = "OUTFLOWS", "OUTFLOW",
        BLANK()
    )

-- Core flows
Net Flow =
VAR Inflow  = CALCULATE(SUM(Cashflows[Value]), Cashflows[TypeNorm] = "INFLOW")
VAR Outflow = CALCULATE(SUM(Cashflows[Value]), Cashflows[TypeNorm] = "OUTFLOW")
RETURN COALESCE(Inflow, 0) - COALESCE(Outflow, 0)

Cumulative Net Flow =
VAR Y = MAX(YearTable[Year])
RETURN CALCULATE([Net Flow], FILTER(ALLSELECTED(YearTable[Year]), YearTable[Year] <= Y))

-- Nominal return logic from globals
Nominal Return Rate :=
VAR r = COALESCE([Real Return Rate], 0)
VAR i = COALESCE([Inflation Rate], 0)
RETURN (1 + r) * (1 + i) - 1

-- Growth-aware net worth (compounds base and each flow to Year)
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
- **Table:** Cashflows by category and year, include `Type`, `Account`, `Symbol`, `Spendable?`

---

## Dividend, Tax, and Cashflow Rules (Updated 2025-10-14)

These capture decisions we finalized today.

### 1) Projection Horizon (“lifetime” support)
- `retirement_assumptions.csv` has `Start_Year` and `Duration`.  
- If `Duration = "lifetime"`, the loader expands the series through **End_Year = 2100** (configurable in `ret_globals.end_year`).  
- Otherwise, `End_Year = Start_Year + Duration - 1`.

### 2) Dividend engine and growing balances
- Annual **Dividends** for each account-year are computed from the **projected end‑of‑year balance** (not a static snapshot):  
  	`dividends_y = balance_y * forward_or_trailing_yield(symbol)`
- `balance_y` is derived via rolling compounding of: prior balance, **contributions/withdrawals**, and **nominal return**.  
  This ensures that increasing contributions (e.g., BRK_JOINT from 30k → 150k) **does** raise dividends by 2040 and beyond.

### 3) Yield sources
- `security_dim.csv` holds `yield_forward` (preferred) and `yield_trailing`.  
- If `yield_forward` is null, fall back to `yield_trailing`.  
- A `last_verified` column tracks data freshness. (Populate via your symbol‑scrape step.)

### 4) Qualified dividend ratio & muni/treasury handling
- Column: `qualified_ratio` ∈ [0.0–1.0], representing the share taxed at qualified rates.  
- **Muni funds (e.g., VOHIX)**: set `tax_treatment = "muni"`, and **ignore** `qualified_ratio` in tax math; dividends are **federal tax‑exempt** and typically **state‑exempt** if in‑state (document your state rule).  
- **Treasuries**: `tax_treatment = "treasury"` for **state‑tax‑exempt** interest.  
- **Sentinel vs zero**: avoid using `0` to mean “override to zero tax.” Use either `tax_treatment` or a sentinel like `qualified_ratio = 999` (the loader treats `999` as “override → zero tax”). Prefer the explicit `tax_treatment` enum.

### 5) Spendability (pre‑retirement cash)
- A boolean **Spendable_Pre_Ret** is derived in the model:  
  	`Spendable_Pre_Ret = (account_dim.acct_group = "Liquid")`  
- In Power BI, the **Dividends (Spendable Pre‑Ret)** measure filters by this flag for pre‑retirement cash‑flow views.

### 6) Cashflows typing and normalization
- `Cashflows[Type]` must be strictly `"INFLOW"` or `"OUTFLOW"` (plural variants normalized by `TypeNorm` above).  
- Dividends are **inflows**; withdrawals are **outflows**. Social Security, contributions, etc., are loaded into the correct side upstream in `load_retirement.py`.

### 7) Globals for return and inflation
- `ret_globals` provides:  
  - `real_return_rate` (e.g., 0.045)  
  - `inflation_rate` (e.g., 0.02)  
  - `end_year` (default `2100` for lifetime expansion)  
- In DAX we compute `Nominal Return Rate = (1+r_real)*(1+i) - 1` and compound flows/year accordingly.

### 8) Budgets roll‑forward (clarified)
- `budgets.csv` is the **source of truth** (not stored in DuckDB).  
- If the row for the **current month** is blank, the loader **copies forward** the prior month’s values on ingest.  
- This keeps visuals populated without manual monthly edits.

### 9) Troubleshooting notes
- **Net worth line drops pre‑retirement:** ensure `Nominal Return Rate` is wired and the growth‑aware net‑worth measure above is used.  
- **Dividends stop early (e.g., 2065):** check `Duration` vs `"lifetime"` and `ret_globals.end_year`.  
- **Contribution changes not impacting dividends:** verify the rolling balance calc is enabled in `load_retirement.py` and that dividends use **projected** balances.

---

## File Locations & Conventions

- **Manual Positions:** `FinanceData/positions/normalized/manual/positions_<YYYY-MM-DD>.csv`  
  Update by duplicating the most recent file with a new date-stamped name.

- **Retirement Assumptions:** `FinanceData/retirement/retirement_assumptions.csv`  
  Expanded into `ret_inflows.csv` & `ret_outflows.csv` with lifetime support.

- **Globals:** `rules/ret_globals.csv` (or table) controlling `real_return_rate`, `inflation_rate`, `end_year`.

---

## Roadmap

- Toggle between **Nominal vs Real** views in Power BI.  
- Scenario picker for returns (e.g., 4% / 6% / 8%).  
- Withdrawal sequencing and tax‑bucket logic (Taxable / Roth / Trad).  
- RMD projections and Social Security tax treatment.  
- Automated yield refresh step writing `security_dim.yield_forward` + `last_verified`.

---

## Quick Commands

```powershell
# Full refresh
./run_loaders.ps1

# Only retirement
python src/etl/load_retirement.py

# Only rollups
python src/etl/build_rollups.py

# Inspect a view
python .\src\etl\run_sql.py "SELECT * FROM ret_outflows LIMIT 50"
```

---

### Changelog
- **2025-10-14:** Added lifetime expansion (to 2100), rolling-balance‑based dividends, muni/treasury tax‐treatment logic, spendability flag, and clarified budgets roll‑forward. Updated DAX and troubleshooting.
- **Prior:** Initial setup, rules/positions loaders, nominal return integration, manual positions ingestion.


# 9) Federal Tax Estimation Module (2025)

This section documents the simplified **federal income tax estimator** used for quarterly planning.  
It computes ordinary income tax using full 2025 MFJ brackets and applies LTCG rates to qualified dividends.

## 9.1 Inputs (Power BI)

### Table: `annual_tax_est` (1 row per metric)
Required fields:
- `w2_income`
- `deductions` (negative; e.g., -31500)
- `loss_offset_ordinary` (e.g., -3000 TLH offset)
- `w2_taxes` (federal withheld YTD)
- `est_tax_payments` (quarterly estimated payments YTD)

### Dividend Measures (from portfolio model)
- `[Est Annual Ordinary Dividends (Taxable)]`
- `[Est Annual Qualified Dividends (Taxable)]`

## 9.2 Derived Measures (DAX)

### Ordinary Income Base
```
Ordinary Income Base :=
[W2 Income] +
[Ordinary Dividends (Taxable)] +
[Loss Offset Ordinary] +
[Deductions]
```

### Federal Ordinary Income Tax (2025 MFJ Brackets)
```
Federal Ordinary Tax :=
VAR TI = [Ordinary Income Base]
VAR Tax =
    SWITCH(
        TRUE(),
        TI <= 0, 0,
        TI <= 23850, TI * 0.10,
        TI <= 96950, 2385 + (TI - 23850) * 0.12,
        TI <= 206700, 11157 + (TI - 96950) * 0.22,
        TI <= 394600, 35302 + (TI - 206700) * 0.24,
        TI <= 501050, 80398 + (TI - 394600) * 0.32,
        TI <= 751600, 114462 + (TI - 501050) * 0.35,
        191011 + (TI - 751600) * 0.37
    )
RETURN Tax
```

### Qualified Dividend Tax (LTCG + NIIT)
```
Qualified Effective Rate := 0.188   -- 15% CG + 3.8% NIIT

Federal Qualified Tax :=
[Qualified Dividends (Taxable)] * [Qualified Effective Rate]
```

### Total Federal Liability
```
Projected Federal Tax :=
[Federal Ordinary Tax] + [Federal Qualified Tax]
```

### Remaining Liability (Amount Still Owed)
```
Remaining Federal Liability :=
[Projected Federal Tax] -
([W2 Federal Taxes Paid] + [Estimated Payments YTD])
```

This is a fully automated federal projection model with correct bracket mechanics and clean integration of dividend taxation.
