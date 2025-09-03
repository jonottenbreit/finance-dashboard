# Finance Dashboard

Custom personal finance dashboard for my family.

**Stack:**
- Python (ETL + transforms)
- DuckDB (analytics database)
- Power BI Project (.pbip) for reporting
- Data files (transactions, balances) live in OneDrive — not in Git

## Repo Layout
- `src/` → ETL scripts (`extract`, `transform`, `snapshots`)
- `models/` → calculations, metrics, categorization rules
- `powerbi/` → Power BI project (.pbip)
- `data/` → local data cache (ignored by Git, stored in OneDrive)
- `docs/` → decisions, design notes
- `notebooks/` → exploration / prototyping
- `tests/` → unit tests

## Decisions
- Hybrid stack: aggregator feeds → Python/DuckDB → Power BI
- GitHub repo versions code + Power BI project; **no raw data** in Git
- Use pre-commit hooks (Black, Ruff, Jupytext) for clean diffs
- Track architecture changes in `docs/decisions.md`

---

## Next Steps
- Set up Python venv + install basics (`duckdb`, `pandas`, `python-dotenv`)
- Add ETL scripts to load transactions into DuckDB
- Build initial Power BI model and save as `.pbip`

