import duckdb
from pathlib import Path

DB_PATH = Path(r"C:\Users\jo136\OneDrive\FinanceData\finance.duckdb")

con = duckdb.connect(DB_PATH)

# Check overrides actually loaded
print("Overrides loaded:")
print(con.execute("""
SELECT active, date, description_regex, amount, category
FROM category_overrides
""").df())

print(con.execute("""
WITH base AS (
  SELECT
    ROW_NUMBER() OVER () AS rid,
    LOWER(COALESCE(description, '')) AS desc_src,
    date  AS b_date,
    amount AS b_amount
  FROM transactions
  WHERE LOWER(COALESCE(description, '')) LIKE '%amazon%'
),
ov AS (SELECT * FROM category_overrides WHERE active = TRUE)
SELECT
  b.rid, b.b_date, b.b_amount, b.desc_src,
  o.date AS o_date, o.amount AS o_amount, o.description_regex, o.category AS o_category,
  regexp_matches(b.desc_src, o.description_regex)    AS m_regex,
  (o.date IS NULL OR b.b_date = o.date)              AS m_date,
  (b.b_amount = o.amount)                            AS m_amt_exact,
  (ABS(b.b_amount) = ABS(o.amount))                  AS m_amt_abs
FROM base b
JOIN ov o
  ON (o.date IS NULL OR b.b_date = o.date)
""").df())




