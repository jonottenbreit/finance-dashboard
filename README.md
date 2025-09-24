# Finance Dashboard

Custom personal finance dashboard.

## Stack
- **Python** ETL + **DuckDB** (compute/storage)
- **Parquet** exports for **Power BI**
- Data in **OneDrive**; code in Git (Power BI as .pbip)

---

## Paths
- **Repo**: `C:\Users\jo136\Projects\finance-dashboard`
- **Data dir** (from `.env`): `C:\Users\jo136\OneDrive\FinanceData`
- **DuckDB**: `C:\Users\jo136\OneDrive\FinanceData\finance.duckdb`

---

## Data Flow (Bronze → Silver → Gold)

### Bronze (raw files)
Drop institutional exports under:
```
C:\Users\jo136\OneDrive\FinanceData\positions\raw\<vendor>\<route>\YYYY-MM-DD-positions.csv
```
Current sources:
- `chase\brokerage_taxable` → **account_id = BRK_JOINT**
- `chase\IRA_JON` → **account_id = IRA_JON**
- `alight\all_accounts` → **account_id = 401K_JON**

Notes:
- Chase files may include **FOOTNOTES** sections — trimmed automatically.
- Alight uses **Fund Name** instead of ticker; we **slug** it to a stable `symbol` (e.g., “Vanguard Target 2050” → `VANGUARD_TARGET_2050`). Recognizes **Closing Balance / Ending Balance / Current Balance** as the dollar value. Parentheses negatives supported.

### Silver (normalized snapshots)
Normalizer writes canonical CSVs per date mirroring vendor/route:
```
C:\Users\jo136\OneDrive\FinanceData\positions\normalized\<vendor>\<route>\positions_YYYY-MM-DD.csv
```
Canonical schema (per row = one holding at a snapshot):
```
as_of_date, account_id, symbol, shares, price, market_value
```
Cleaning rules:
- CASH/sweep lines normalized to `symbol=CASH`, `price=1`, `shares=market_value` when unit-only.
- If only a dollar balance exists (unitless), set `price=1`, `shares=market_value`.
- Duplicates collapsed to one row per `(as_of_date, account_id, symbol)`.

### Gold (DuckDB + rollups)
**Table**: `positions(as_of_date DATE, account_id TEXT, symbol TEXT, shares DOUBLE, price DOUBLE, market_value DOUBLE)`  
**Constraint**: unique `(as_of_date, account_id, symbol)`

Loader semantics (**idempotent**):
1. Read normalized files recursively (ignore legacy flat files).  
2. Keep **newest file** per `(as_of_date, account_id, symbol)` using file mtime.  
3. **MERGE (upsert)** → update existing + insert new rows.  
4. **Anti-delete** within each `(as_of_date, account_id)` → remove symbols not present in the new snapshot (handles sells).

### Rules / reference data
- `rules/security_dim.csv` columns **(current)**:  
  `symbol, asset_class, region, style, size, expense_ratio`
- `rules/target_allocation.csv`: `asset_class, target_weight` (weights sum to 1.0)
- `rules/account_dim.csv`: contains `BRK_JOINT`, `IRA_JON`, `401K_JON` and group/tax metadata.

### Rollups & Exports (for Power BI)
Views/tables created by `build_rollups.py` and exported to Parquet in:
```
C:\Users\jo136\OneDrive\FinanceData\exports\
```
Key outputs:
- `positions_enriched_export.parquet`  
  (positions joined to `account_dim` + `security_dim` with **size** and **expense_ratio** included)
- `monthly_allocation.parquet`
- `allocation_vs_target.parquet`
- `monthly_allocation_by_size.parquet` *(drilldown)*
- `monthly_weighted_expense_ratio.parquet` *(metric)*

---

## Power BI Model

### Relationships
- `month_dim[month] (Date)` → `positions_enriched_export[MonthDate] (Date)`  
  *(Add `MonthDate` on fact as first-of-month from `as_of_date`.)*
- Optional: joins to other exports per README’s earlier guidance.

### Core measures (portfolio & breakdowns)
Create on **positions_enriched_export**:

```DAX
Total Value := SUM(positions_enriched_export[value])

Portfolio Total (Selected) := CALCULATE([Total Value], ALLSELECTED('security_dim'))

% of Portfolio := DIVIDE([Total Value], [Portfolio Total (Selected)])

Latest Month := CALCULATE(MAX('month_dim'[month]), ALL('month_dim'))
Total Value (Latest) := CALCULATE([Total Value], 'month_dim'[month] = [Latest Month])
% of Portfolio (Latest) := DIVIDE([Total Value (Latest)],
                                  CALCULATE([Total Value (Latest)], ALLSELECTED('security_dim')))

Weighted Expense Ratio :=
DIVIDE(
  SUMX(positions_enriched_export, positions_enriched_export[value] * positions_enriched_export[expense_ratio]),
  [Total Value]
)
```

Use with `asset_class`, `region`, `style`, `size` on rows/axes; add month/account/tax_bucket slicers.

### Portfolio tab (actionable today)
- **Clustered column**: X = `asset_class`; Y = `% of Portfolio (Latest)` vs. **Target %**.
- **Variance table**: `asset_class`, variance %, **To Buy $ / To Sell $** using portfolio value for latest month.

---

## How to run

```powershell
python src\etl\normalize_positions.py
python src\etl\load_positions.py
python src\etl\build_rollups.py
# then refresh Power BI
```

---

## Next
- Add vendor parsers for new accounts (Fidelity, Vanguard, etc.).
- Expand `security_dim.csv` mappings for Alight fund slugs.
- Optional: ingest manifest (file hash) to skip unchanged raw files.
