# Next steps — Finance Dashboard

## Completed
- Category & budget loaders (`rules/category_dim.csv`, `rules/budget_monthly.csv`)
- Combined `build_rollups.py` exporting: budget, categories, actuals-by-category, month_dim, monthly_cashflow
- Power BI model wired with relationships
- Measures added under `Measures_Main`

## Next (small bites)
- Add any missing raw categories to `rules/category_dim.csv` to eliminate "Unknown" rows; reload.
- (Optional) Add `rules/category_map.csv` to map raw labels (e.g., “Income” → “Salary”) and patch rollups to apply the map.
- Create Budget vs Actual **Matrix** and a **Cashflow** line on separate report pages.
- Seed `balance_snapshot` + export `monthly_net_worth.parquet` → Net Worth page.

## Runbook (typical refresh)
```powershell
cd C:\Users\jo136\Projects\finance-dashboard
.\.venv\Scripts\Activate
python src\etl\load_csv.py
python src\etl\load_categories_and_budget.py
python src\etl\build_rollups.py
# then Refresh in Power BI
```
