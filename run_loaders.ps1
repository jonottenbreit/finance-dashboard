# run_loaders.ps1
$ErrorActionPreference = "Stop"

# 1) Activate venv
.\.venv\Scripts\Activate

# 2) Load sources (each script should be idempotent)

Write-Host "Loading rules..."
python src\etl\load_rules.py

Write-Host "Normalizing transactions..."
python src\etl\normalize_transactions.py 

Write-Host "Loading transactions..."
python src\etl\load_transactions.py

Write-Host "Normalizing positions..."
python src\etl\normalize_positions.py 

Write-Host "Loading positions..."
python src\etl\load_positions.py  # remove if you don't maintain positions

Write-Host "Loading accounts and balances..."
python src\etl\load_accounts_and_balances.py # Loads the account balances over time

# If your build script auto-loads rules from CSVs, you can skip explicit rules loading.
# Otherwise, add tiny loaders for security_dim/target_allocation as needed.

# 3) Build exports
Write-Host "Building rollups..."
python src\etl\build_rollups.py

Write-Host "=== Rolling forward budgets (if needed) ==="
python src/etl/budget_roll_forward_csv_only.py `
  --budgets "C:\Users\jo136\OneDrive\FinanceData\budgets\budgets.csv" `
  --tx "C:\Users\jo136\OneDrive\FinanceData\exports\transactions_with_category.parquet"

Write-Host "All loaders completed."
