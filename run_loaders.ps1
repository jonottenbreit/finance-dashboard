# run_loaders.ps1
$ErrorActionPreference = "Stop"

# 1) Activate venv
.\.venv\Scripts\Activate

# 2) Load sources (each script should be idempotent)

Write-Host "Loading rules..."
python src\etl\load_rules.py

Write-Host "Loading transactions..."
python src\etl\load_transactions.py

Write-Host "Loading positions..."
python src\etl\load_positions.py  # remove if you don't maintain positions

Write-Host "Loading accounts and balances..."
python src\etl\load_accounts_and_balances.py # Loads the account balances over time

# If your build script auto-loads rules from CSVs, you can skip explicit rules loading.
# Otherwise, add tiny loaders for security_dim/target_allocation as needed.

# 3) Build exports
Write-Host "Building rollups..."
python src\etl\build_rollups.py

Write-Host "All loaders completed."
