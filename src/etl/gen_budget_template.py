from pathlib import Path
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta  # pip install python-dateutil

# Where to write the CSV
REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "rules" / "budget_monthly.csv"

# ---- EDIT THESE ----
START = date(2025, 1, 1)   # first month you want
MONTHS = 24                # how many months to create

AMOUNTS = {
    "Salary": 9000,
    "Mortgage": 2200,
    "Utilities": 250,
    "Groceries": 800,
    "Restaurants": 300,
    "Endurance Sports": 200,
    "Clothing": 150,
    "Travel": 300,
    "Self Care": 100,
    "401k Contribution": 1500,
}
# --------------------

rows = []
m = START
for _ in range(MONTHS):
    for cat, amt in AMOUNTS.items():
        rows.append({"month": m.isoformat(), "category": cat, "amount": amt})
    m += relativedelta(months=1)

df = pd.DataFrame(rows, columns=["month", "category", "amount"])
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} ({len(df)} rows)")
