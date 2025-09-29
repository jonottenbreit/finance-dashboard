"""
Build reporting rollups and export Parquet snapshots.
Run:
    python src/etl/build_rollups.py
"""
# src/etl/build_rollups.py
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

# -------------------------
# ENV / PATHS
# -------------------------
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\jo136\OneDrive\FinanceData"))
DB_ENV = os.getenv("DUCKDB_PATH")
DB_PATH = Path(DB_ENV) if DB_ENV else (DATA_DIR / "finance.duckdb")
EXPORTS_DIR = DATA_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))

# -------------------------
# HELPERS
# -------------------------
def has_table(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        return con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
            [name],
        ).fetchone() is not None
    except Exception:
        return False

def has_column(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    try:
        return con.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ? LIMIT 1",
            [table, col],
        ).fetchone() is not None
    except Exception:
        return False

def safe_copy(table: str, filename: str):
    if has_table(con, table):
        dst = (EXPORTS_DIR / filename).as_posix()
        con.execute(f"COPY {table} TO '{dst}' (FORMAT PARQUET)")
        print(f"Exported {table} -> {dst}")
    else:
        print(f"SKIP export {table}: table not found")

# -------------------------
# Ensure rules tables exist (from CSVs) if missing
# -------------------------
rules_dir = (Path(os.getenv("RULES_DIR")) if os.getenv("RULES_DIR")
             else Path(__file__).resolve().parents[2] / "rules")

# security_dim (create if missing, else add new cols if needed)
if not has_table(con, "security_dim"):
    sec_csv = rules_dir / "security_dim.csv"
    if sec_csv.exists():
        con.execute("""
            CREATE TABLE security_dim (
              symbol TEXT PRIMARY KEY,
              asset_class TEXT,
              region TEXT,
              style TEXT,
              size TEXT,
              expense_ratio DECIMAL(9,6)
            )
        """)
        con.execute("INSERT INTO security_dim SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(sec_csv)])
        print("Loaded security_dim from CSV")
else:
    if not has_column(con, "security_dim", "size"):
        con.execute("ALTER TABLE security_dim ADD COLUMN size TEXT;")
    if not has_column(con, "security_dim", "expense_ratio"):
        con.execute("ALTER TABLE security_dim ADD COLUMN expense_ratio DECIMAL(9,6);")

# target_allocation
if not has_table(con, "target_allocation"):
    targ_csv = rules_dir / "target_allocation.csv"
    if targ_csv.exists():
        con.execute("""
            CREATE TABLE target_allocation (
              asset_class TEXT PRIMARY KEY,
              target_weight DOUBLE
            )
        """)
        con.execute("INSERT INTO target_allocation SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(targ_csv)])
        print("Loaded target_allocation from CSV")

# category_dim (optional but recommended)
if not has_table(con, "category_dim"):
    catdim_csv = rules_dir / "category_dim.csv"
    if catdim_csv.exists():
        # Be permissive on columns; read_csv_auto will infer what’s there.
        con.execute("CREATE TABLE category_dim AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(catdim_csv)])
        print("Loaded category_dim from CSV")

# category_rules WITHOUT subcategory (we will ignore any subcategory col if present)
# Expected minimal columns: rule_id (optional), match_type, pattern, category
if not has_table(con, "category_rules"):
    catrules_csv = rules_dir / "category_rules.csv"
    if catrules_csv.exists():
        con.execute("CREATE TABLE category_rules AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(catrules_csv)])
        print("Loaded category_rules from CSV")

# -------------------------
# MONTH DIM (union of sources present)
# -------------------------
unions = []

if has_table(con, "transactions") and has_column(con, "transactions", "date"):
    unions.append("SELECT date_trunc('month', date)::DATE AS m FROM transactions")

if has_table(con, "budget_monthly") and has_column(con, "budget_monthly", "month"):
    unions.append("SELECT date_trunc('month', month)::DATE AS m FROM budget_monthly")

# balances
if has_table(con, "balance_snapshot") and has_column(con, "balance_snapshot", "as_of_date"):
    unions.append("SELECT date_trunc('month', as_of_date)::DATE AS m FROM balance_snapshot")

# positions
if has_table(con, "positions") and has_column(con, "positions", "as_of_date"):
    unions.append("SELECT date_trunc('month', as_of_date)::DATE AS m FROM positions")

if unions:
    con.execute(f"""
        CREATE OR REPLACE TABLE month_dim AS
        SELECT DISTINCT m::DATE AS month
        FROM (
            {' UNION ALL '.join(unions)}
        )
        ORDER BY 1;
    """)
    print("Built month_dim")
else:
    print("SKIP month_dim: no date-bearing sources found")

# -------------------------
# CATEGORY ASSIGNMENT PIPELINE (no subcategory)
# -------------------------
# Build a single source of truth for categories:
# 1) If transactions already carry a 'category' column, we keep it.
# 2) Else fall back to applying category_rules (no subcategory) on description-like fields when possible.
# 3) Enrich with category_dim (if present) to expose higher-level groupings; all optional/guarded.

# Identify description-like columns we may match on
desc_cols = [c for c in ("clean_description", "description", "memo", "payee", "name") if has_column(con, "transactions", c)]
desc_expr = None
if desc_cols:
    # prefer clean_description > description > memo > payee > name
    for pref in ("clean_description", "description", "memo", "payee", "name"):
        if pref in desc_cols:
            desc_expr = pref
            break

# Create a VIEW transactions_with_category that guarantees a 'category' even if rules are needed
# Note: we ignore any 'subcategory' concept entirely.
if has_table(con, "transactions"):
    # If rules exist and we have a description column, support two match types:
    # - 'regex' -> pattern is a regex applied to description
    # - 'contains' -> case-insensitive substring
    rules_available = has_table(con, "category_rules") and desc_expr is not None and \
        all(has_column(con, "category_rules", c) for c in ("match_type", "pattern", "category"))

    if rules_available and not has_column(con, "transactions", "category"):
        # Build rule application
        con.execute(f"""
            CREATE OR REPLACE VIEW transactions_with_category AS
            WITH rules AS (
              SELECT
                COALESCE(LOWER(match_type),'contains') AS match_type,
                pattern,
                category
              FROM category_rules
              WHERE category IS NOT NULL AND TRIM(category) <> ''
            ),
            base AS (
              SELECT *, {desc_expr} AS desc_src FROM transactions
            ),
            hits AS (
              SELECT
                b.*,
                r.category,
                ROW_NUMBER() OVER (PARTITION BY b.rowid ORDER BY r.category) AS rn
              FROM base b
              JOIN rules r
                ON (
                  (r.match_type = 'regex' AND regexp_matches(LOWER(b.desc_src), r.pattern))
                  OR
                  (r.match_type = 'contains' AND POSITION(LOWER(r.pattern) IN LOWER(b.desc_src)) > 0)
                )
            )
            SELECT
              COALESCE(h.category, 'Uncategorized') AS category,
              b.*
            FROM base b
            LEFT JOIN (
              SELECT * FROM hits WHERE rn = 1
            ) h
            ON h.rowid = b.rowid
        """)
        print("Built transactions_with_category via rules (no subcategory)")
    else:
        # Either we already have a 'category' column or cannot apply rules; just pass through
        cat_col = "category" if has_column(con, "transactions", "category") else "NULL AS category"
        con.execute(f"""
            CREATE OR REPLACE VIEW transactions_with_category AS
            SELECT
              {cat_col},
              t.*
            FROM transactions t
        """)
        print("Built transactions_with_category passthrough")
else:
    print("SKIP: transactions table missing; cannot build transactions_with_category")

# Enrich categories with higher-level groupings if category_dim exists (all optional columns)
# We'll create a VIEW category_enriched: category, level1, level2, level3 (whatever exists)
if has_table(con, "category_dim"):
    # Probe optional higher-level columns
    lvl_cols = [c for c in ("level1", "level2", "level3", "parent", "group_1", "group_2") if has_column(con, "category_dim", c)]
    select_lvls = ", ".join([f"cd.{c} AS {c}" for c in lvl_cols]) if lvl_cols else ""
    comma = ", " if select_lvls else ""
    con.execute(f"""
        CREATE OR REPLACE VIEW category_enriched AS
        SELECT
          cd.category{comma}{select_lvls}
        FROM category_dim cd
    """)
    print("Built category_enriched view (optional higher-levels)")
else:
    print("SKIP category_enriched: category_dim not found")

# -------------------------
# CASHFLOW ROLLUP
# -------------------------
if has_table(con, "transactions_with_category") and has_column(con, "transactions_with_category", "amount_cents") and has_column(con, "transactions_with_category", "date"):
    where_clause = "WHERE COALESCE(is_transfer, FALSE) = FALSE" if has_column(con, "transactions_with_category", "is_transfer") else ""

    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_cashflow AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', date) AS month,
            CAST(amount_cents/100.0 AS DOUBLE) AS amt
          FROM transactions_with_category
          {where_clause}
        )
        SELECT
          month,
          SUM(CASE WHEN amt > 0 THEN amt ELSE 0 END) AS income,
          SUM(CASE WHEN amt < 0 THEN amt ELSE 0 END) AS spending,  -- negative
          SUM(amt) AS net_cashflow
        FROM t
        GROUP BY 1
        ORDER BY 1;
    """)
    print("Built monthly_cashflow")
else:
    print("SKIP monthly_cashflow: needed columns not found")

# -------------------------
# ACTUALS BY CATEGORY (P&L detail; no subcategory)
# -------------------------
if has_table(con, "transactions_with_category") and has_column(con, "transactions_with_category", "category") and has_column(con, "transactions_with_category", "date") and has_column(con, "transactions_with_category", "amount_cents"):
    where_clause = "WHERE COALESCE(is_transfer, FALSE) = FALSE" if has_column(con, "transactions_with_category", "is_transfer") else ""

    # Base (category only)
    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_actuals_by_category AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', date) AS month,
            COALESCE(category, 'Uncategorized') AS category,
            CAST(amount_cents/100.0 AS DOUBLE) AS amt
          FROM transactions_with_category
          {where_clause}
        )
        SELECT
          month,
          category,
          SUM(amt)                                   AS actual_signed,         -- keep signs
          SUM(CASE WHEN amt < 0 THEN -amt ELSE 0 END) AS spending              -- positive spend
        FROM t
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    print("Built monthly_actuals_by_category")

    # Optional: enriched rollup if category_dim exists (adds higher-levels, still no subcategory)
    if has_table(con, "category_enriched"):
        # detect which higher-level columns exist and stitch them in
        lvl_cols = [c for c in ("level1", "level2", "level3", "parent", "group_1", "group_2")
                    if has_column(con, "category_dim", c)]
        select_lvls = ", ".join([f"ce.{c}" for c in lvl_cols]) if lvl_cols else ""
        comma = ", " if select_lvls else ""
        group_lvls = ("," + ",".join([f"ce.{c}" for c in lvl_cols])) if lvl_cols else ""

        con.execute(f"""
            CREATE OR REPLACE TABLE monthly_actuals_by_category_enriched AS
            WITH t AS (
              SELECT
                strftime('%Y-%m', twc.date) AS month,
                COALESCE(twc.category, 'Uncategorized') AS category,
                CAST(twc.amount_cents/100.0 AS DOUBLE) AS amt
              FROM transactions_with_category twc
              {where_clause}
            )
            SELECT
              t.month,
              t.category{comma}{select_lvls},
              SUM(t.amt)                                   AS actual_signed,
              SUM(CASE WHEN t.amt < 0 THEN -t.amt ELSE 0 END) AS spending
            FROM t
            LEFT JOIN category_enriched ce ON ce.category = t.category
            GROUP BY t.month, t.category{group_lvls}
            ORDER BY 1,2;
        """)
        print("Built monthly_actuals_by_category_enriched")
else:
    print("SKIP monthly_actuals_by_category: needed columns missing")

# -------------------------
# NET WORTH ROLLUPS
# -------------------------
if has_table(con, "balance_snapshot") and has_table(con, "account_dim"):
    con.execute("""
        CREATE OR REPLACE VIEW balances_enriched AS
        SELECT
          date_trunc('month', b.as_of_date)::DATE AS month_date,
          strftime('%Y-%m', b.as_of_date)        AS month,
          b.account_id,
          a.account_name,
          a.type,
          a.acct_group,
          a.tax_bucket,
          a.liquidity,
          a.include_networth,
          a.include_liquid,
          CASE WHEN LOWER(COALESCE(a.type,'')) = 'liability'
               THEN -1.0 * CAST(b.balance AS DOUBLE)
               ELSE CAST(b.balance AS DOUBLE)
          END AS balance_norm
        FROM balance_snapshot b
        LEFT JOIN account_dim a USING(account_id);
    """)
    con.execute("""
        CREATE OR REPLACE TABLE monthly_net_worth AS
        SELECT
          month,
          SUM(CASE WHEN include_networth THEN balance_norm ELSE 0 END)                           AS net_worth,
          SUM(CASE WHEN include_networth AND balance_norm > 0 THEN balance_norm ELSE 0 END)     AS assets,
          SUM(CASE WHEN include_networth AND balance_norm < 0 THEN balance_norm ELSE 0 END)     AS liabilities,
          SUM(CASE WHEN include_liquid    THEN balance_norm ELSE 0 END)                          AS liquid_net_worth,
          SUM(CASE WHEN include_networth AND liquidity IN ('investable') THEN balance_norm ELSE 0 END) AS investable_assets
        FROM balances_enriched
        GROUP BY 1
        ORDER BY 1;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE monthly_net_worth_by_group AS
        SELECT
          month,
          acct_group,
          SUM(CASE WHEN include_networth THEN balance_norm ELSE 0 END) AS value
        FROM balances_enriched
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    print("Built monthly_net_worth & monthly_net_worth_by_group")
else:
    print("SKIP net worth: balance_snapshot or account_dim missing")

# -------------------------
# ALLOCATION (positions + security_dim + account_dim)
# -------------------------
if has_table(con, "positions"):
    join_sec = "LEFT JOIN security_dim s USING(symbol)" if has_table(con, "security_dim") else "LEFT JOIN (SELECT NULL) s ON FALSE"
    join_acct = "LEFT JOIN account_dim a USING(account_id)" if has_table(con, "account_dim") else "LEFT JOIN (SELECT NULL) a ON FALSE"

    con.execute(f"""
        CREATE OR REPLACE VIEW positions_enriched AS
        SELECT
          p.as_of_date,
          strftime('%Y-%m', p.as_of_date) AS month,
          p.account_id,
          COALESCE(a.acct_group, 'Unknown')   AS acct_group,
          COALESCE(a.tax_bucket, 'Unknown')   AS tax_bucket,
          COALESCE(a.liquidity, 'Unknown')    AS liquidity,
          p.symbol,
          COALESCE(s.asset_class, 'Unknown')  AS asset_class,
          COALESCE(s.region, 'Unknown')       AS region,
          COALESCE(s.style, 'Unknown')        AS style,
          COALESCE(s.size, 'Unknown')         AS size,
          COALESCE(s.expense_ratio, 0.0)      AS expense_ratio,
          CAST(p.market_value AS DOUBLE)      AS value
        FROM positions p
        {join_sec}
        {join_acct};
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_allocation AS
        SELECT
          month,
          asset_class,
          region,
          style,
          tax_bucket,
          SUM(value) AS value
        FROM positions_enriched
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_allocation_by_size AS
        SELECT
          month,
          asset_class,
          region,
          style,
          size,
          tax_bucket,
          SUM(value) AS value
        FROM positions_enriched
        GROUP BY 1,2,3,4,5,6
        ORDER BY 1,2,5;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE monthly_weighted_expense_ratio AS
        SELECT
          month,
          asset_class,
          SUM(value * expense_ratio) / NULLIF(SUM(value), 0) AS weighted_expense_ratio
        FROM positions_enriched
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW investable_by_month AS
        SELECT month, SUM(value) AS investable_value
        FROM positions_enriched
        GROUP BY month;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW allocation_weights AS
        SELECT
          m.month,
          m.asset_class,
          SUM(m.value) AS value,
          i.investable_value,
          SUM(m.value) / NULLIF(i.investable_value, 0) AS actual_weight
        FROM monthly_allocation m
        JOIN investable_by_month i USING(month)
        GROUP BY 1,2,4;
    """)

    if has_table(con, "target_allocation"):
        con.execute("""
            CREATE OR REPLACE TABLE allocation_vs_target AS
            SELECT
              w.month,
              w.asset_class,
              w.actual_weight,
              t.target_weight,
              w.actual_weight - t.target_weight AS variance
            FROM allocation_weights w
            LEFT JOIN target_allocation t USING(asset_class)
            ORDER BY w.month, w.asset_class;
        """)
    else:
        print("INFO: allocation_vs_target skipped (no target_allocation)")

    con.execute("""
        CREATE OR REPLACE TABLE positions_enriched_export AS
        SELECT
            as_of_date,
            month,
            account_id,
            acct_group,
            tax_bucket,
            liquidity,
            symbol,
            asset_class,
            region,
            style,
            size,
            expense_ratio,
            CAST(value AS DOUBLE) AS value
        FROM positions_enriched
        ORDER BY month, account_id, symbol
    """)

    print("Built allocation tables")
else:
    print("SKIP allocation: positions table not found")

# -------------------------
# EXPORTS (Parquet)
# -------------------------
safe_copy("transactions_with_category", "transactions_with_category.parquet")
safe_copy("monthly_cashflow", "monthly_cashflow.parquet")
safe_copy("monthly_actuals_by_category", "monthly_actuals_by_category.parquet")
safe_copy("monthly_actuals_by_category_enriched", "monthly_actuals_by_category_enriched.parquet")
safe_copy("budget_monthly", "budget_monthly.parquet")
safe_copy("category_dim", "category_dim.parquet")
safe_copy("security_dim", "security_dim.parquet")
safe_copy("month_dim", "month_dim.parquet")

safe_copy("monthly_net_worth", "monthly_net_worth.parquet")
safe_copy("monthly_net_worth_by_group", "monthly_net_worth_by_group.parquet")

safe_copy("monthly_allocation", "monthly_allocation.parquet")
safe_copy("allocation_vs_target", "allocation_vs_target.parquet")
safe_copy("positions_enriched_export", "positions_enriched_export.parquet")

print("Done.")
