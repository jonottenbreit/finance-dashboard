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

#4) Retirement Loader

### ---- Retirement: load assumptions + derive flows ----
Write-Host "=== Retirement: load assumptions + derive flows ==="

# OneDrive data locations (inputs/outputs)
$DATA_DIR = ${env:DATA_DIR}
if ([string]::IsNullOrEmpty($DATA_DIR)) { $DATA_DIR = "C:\Users\jo136\OneDrive\FinanceData" }

$RET_DIR  = Join-Path $DATA_DIR "retirement"
$OUT_DIR  = Join-Path $RET_DIR  "out"
$CSV_IN   = Join-Path $RET_DIR  "retirement_assumptions.csv"

# Script location (repo relative)
$SCRIPT = Join-Path $PSScriptRoot "src\etl\load_retirement.py"

# Sanity checks
if (!(Test-Path $SCRIPT)) {
  Write-Error "Retirement script not found: $SCRIPT"
  Write-Host  "Searching for *load_retirement.py* under repo root:"
  Get-ChildItem -Path $PSScriptRoot -Recurse -Filter load_retirement.py | Select-Object FullName
  exit 2
}
if (!(Test-Path $CSV_IN)) {
  Write-Error "Retirement assumptions CSV not found: $CSV_IN"
  exit 2
}
if (!(Test-Path $OUT_DIR)) { New-Item -ItemType Directory -Force -Path $OUT_DIR | Out-Null }

# Python (prefer repo venv)
$PY = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (!(Test-Path $PY)) { $PY = "python" }

Write-Host "Using script: $SCRIPT"
Write-Host "Using CSV   : $CSV_IN"
Write-Host "Output dir  : $OUT_DIR"
Write-Host "Python      : $PY"

# Run
& $PY -u "$SCRIPT" --csv "$CSV_IN" --outdir "$OUT_DIR"
if ($LASTEXITCODE -ne 0) {
  Write-Error "Retirement loader failed with exit code $LASTEXITCODE"
  exit $LASTEXITCODE
}

Write-Host "Retirement globals  -> $(Join-Path $OUT_DIR 'ret_globals.csv')"
Write-Host "Retirement inflows  -> $(Join-Path $OUT_DIR 'ret_inflows.csv')"
Write-Host "Retirement outflows -> $(Join-Path $OUT_DIR 'ret_outflows.csv')"
Write-Host "Retirement balances -> $(Join-Path $OUT_DIR 'ret_starting_balances.csv')"


