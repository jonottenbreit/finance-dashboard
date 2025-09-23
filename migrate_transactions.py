import duckdb, os

db = os.getenv("DUCKDB_PATH", r"C:\Users\jo136\Projects\finance-dashboard\finance.duckdb")
con = duckdb.connect(db)

cols = {r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'transactions'
""").fetchall()}

def add(col, typ, default=None):
    if col not in cols:
        sql = f"ALTER TABLE transactions ADD COLUMN {col} {typ}"
        if default is not None:
            sql += f" DEFAULT {default}"
        con.execute(sql)
        print("Added column:", col)

add("amount_cents", "BIGINT")
add("amount", "DOUBLE")
add("merchant_norm", "TEXT")
add("subcategory", "TEXT")
add("memo", "TEXT")
add("tags", "TEXT")
add("is_transfer", "BOOLEAN", "FALSE")

con.execute("""
  UPDATE transactions
  SET amount_cents = CAST(ROUND(amount * 100) AS BIGINT)
  WHERE amount_cents IS NULL
""")

print("Schema after migration:")
print(con.execute("PRAGMA table_info('transactions')").fetchall())
