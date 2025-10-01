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

def list_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    try:
        return [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()]
    except Exception:
        return []

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

# security_dim
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
        con.execute("CREATE TABLE category_dim AS SELECT * FROM read_csv_auto(?, HEADER=TRUE)", [str(catdim_csv)])
        print("Loaded category_dim from CSV")

# category_rules (no subcategory)
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

if has_table(con, "balance_snapshot") and has_column(con, "balance_snapshot", "as_of_date"):
    unions.append("SELECT date_trunc('month', as_of_date)::DATE AS m FROM balance_snapshot")

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
# CATEGORY ASSIGNMENT PIPELINE
# Rules override any default category; single category column; no is_transfer in view
# -------------------------
if has_table(con, "transactions"):
    # Candidate description fields (use ALL via COALESCE so rules still match if one is null)
    desc_candidates = [c for c in ("clean_description","description","memo","payee","name")
                       if has_column(con, "transactions", c)]
    # Build a robust desc_src = lower(coalesce(...))
    if desc_candidates:
        coalesce_expr = "COALESCE(" + ", ".join([f"t.{c}" for c in desc_candidates]) + ", '')"
    else:
        coalesce_expr = "''"  # no description-like cols; matcher will do nothing

    # Columns to expose from transactions (drop duplicates/noise)
    tx_cols_all = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'transactions' ORDER BY ordinal_position"
    ).fetchall()]
    drop_cols = {"subcategory", "is_transfer"}  # exclude these from the view
    tx_cols_no_cat = [c for c in tx_cols_all if c not in (drop_cols | {"category"})]

    has_tx_category = has_column(con, "transactions", "category")

    # Additions for manual overrides
    has_tx_date    = has_column(con, "transactions", "date")
    has_amt_cents  = has_column(con, "transactions", "amount_cents")
    has_amt        = has_column(con, "transactions", "amount")

    overrides_available = (
        has_table(con, "category_overrides") and
        all(has_column(con, "category_overrides", c) for c in ("active","description_regex","amount","category"))
    )

    date_pred   = "(o.date IS NULL OR b.date = o.date)" if has_tx_date else "TRUE"
    if has_amt_cents:
        amount_pred = "CAST(ROUND(o.amount * 100) AS BIGINT) = b.amount_cents"
    elif has_amt:
        amount_pred = "b.amount = o.amount"
    else:
        amount_pred = "TRUE"

    # Rules available?
    rules_ok = (
        has_table(con, "category_rules") and
        all(has_column(con, "category_rules", c) for c in ("match_type","pattern","category"))
    )
    prio_sql = "COALESCE(priority, 9999) AS prio" if has_column(con, "category_rules", "priority") else "9999 AS prio"

    base_select_cols = ", ".join([f"t.{c}" for c in tx_cols_no_cat]) if tx_cols_no_cat else ""
    output_cols = ", ".join([f"b.{c}" for c in tx_cols_no_cat]) if tx_cols_no_cat else ""
    base_cat_col = "t.category AS base_category," if has_tx_category else "'Uncategorized'::TEXT AS base_category,"

    if rules_ok:
        # Build SQL in steps to keep it clean
        sql = f"""
            CREATE OR REPLACE VIEW transactions_with_category AS
            WITH rules AS (
              SELECT
                COALESCE(LOWER(match_type),'contains') AS match_type,
                TRIM(LOWER(pattern)) AS pattern,    -- normalize patterns
                category,
                {prio_sql}
              FROM category_rules
              WHERE category IS NOT NULL AND TRIM(category) <> ''
            ),
            base AS (
              SELECT
                ROW_NUMBER() OVER () AS rid,
                {base_cat_col}
                {base_select_cols}{"," if base_select_cols else ""}
                LOWER({coalesce_expr}) AS desc_src
              FROM transactions t
            )
        """
        if overrides_available:
            sql += f"""
            , ov AS (
              SELECT
                b.rid,
                o.category AS override_category,
                ROW_NUMBER() OVER (PARTITION BY b.rid ORDER BY o.category) AS rn
              FROM base b
              JOIN category_overrides o
                ON o.active = TRUE
               AND regexp_matches(b.desc_src, o.description_regex)
               AND {date_pred}
               AND {amount_pred}
            ),
            overrides_one AS (
              SELECT rid, override_category
              FROM ov WHERE rn = 1
            )
            """
        sql += """
            , hits AS (
              SELECT
                b.rid,
                r.category AS rule_category,
                ROW_NUMBER() OVER (
                  PARTITION BY b.rid
                  ORDER BY r.prio, LENGTH(r.pattern) DESC, r.category
                ) AS rn
              FROM base b
              JOIN rules r
                ON (
                  (r.match_type = 'regex'    AND regexp_matches(b.desc_src, r.pattern))
                  OR
                  (r.match_type = 'contains' AND POSITION(r.pattern IN b.desc_src) > 0)
                )
            )
            SELECT
              COALESCE({override}, h.rule_category, b.base_category, 'Uncategorized') AS category
              {cols}
            FROM base b
            LEFT JOIN (SELECT rid, rule_category FROM hits WHERE rn = 1) h
              ON h.rid = b.rid
            {join_override};
        """.format(
            override="o.override_category" if overrides_available else "NULL",
            cols= ("," + output_cols) if output_cols else "",
            join_override="LEFT JOIN overrides_one o ON o.rid = b.rid" if overrides_available else ""
        )
        con.execute(sql)
        print("Built transactions_with_category (overrides > rules > base)" if overrides_available
              else "Built transactions_with_category (rules override, coalesced desc)")
    else:
        # No usable rules: keep existing category if present, but still honor manual overrides
        category_expr = "t.category" if has_tx_category else "'Uncategorized'::TEXT"
        sql = f"""
            CREATE OR REPLACE VIEW transactions_with_category AS
            WITH base AS (
              SELECT
                ROW_NUMBER() OVER () AS rid,
                {category_expr} AS base_category
                {"," if base_select_cols else ""}{base_select_cols}
                {"," if base_select_cols else ""}LOWER({coalesce_expr}) AS desc_src
              FROM transactions t
            )
        """
        if overrides_available:
            sql += f"""
            , ov AS (
              SELECT
                b.rid,
                o.category AS override_category,
                ROW_NUMBER() OVER (PARTITION BY b.rid ORDER BY o.category) AS rn
              FROM base b
              JOIN category_overrides o
                ON o.active = TRUE
               AND regexp_matches(b.desc_src, o.description_regex)
               AND {date_pred}
               AND {amount_pred}
            ),
            overrides_one AS (
              SELECT rid, override_category
              FROM ov WHERE rn = 1
            )
            """
        sql += """
            SELECT
              COALESCE({override}, b.base_category, 'Uncategorized') AS category
              {cols}
            FROM base b
            {join_override};
        """.format(
            override="o.override_category" if overrides_available else "NULL",
            cols= ("," + output_cols) if output_cols else "",
            join_override="LEFT JOIN overrides_one o ON o.rid = b.rid" if overrides_available else ""
        )
        con.execute(sql)
        print("Built transactions_with_category (overrides > base)" if overrides_available
              else "Built transactions_with_category (passthrough)")
else:
    print("SKIP: transactions table missing; cannot build transactions_with_category")

# Optional: category_enriched (higher-levels)
if has_table(con, "category_dim"):
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
# CASHFLOW ROLLUP (filters transfers via category_dim.is_transfer when present)
# -------------------------
if has_table(con, "transactions_with_category") and has_column(con, "transactions_with_category", "amount_cents") and has_column(con, "transactions_with_category", "date"):
    filter_clause = ""
    if has_table(con, "category_dim") and has_column(con, "category_dim", "category") and has_column(con, "category_dim", "is_transfer"):
        filter_clause = "WHERE COALESCE(cd.is_transfer, FALSE) = FALSE"

    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_cashflow AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', twc.date) AS month,
            CAST(twc.amount_cents/100.0 AS DOUBLE) AS amt,
            twc.category
          FROM transactions_with_category twc
          LEFT JOIN category_dim cd ON cd.category = twc.category
          {filter_clause}
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
# ACTUALS BY CATEGORY (no subcategory; transfer filter via category_dim)
# -------------------------
if has_table(con, "transactions_with_category") and has_column(con, "transactions_with_category", "category") and has_column(con, "transactions_with_category", "date") and has_column(con, "transactions_with_category", "amount_cents"):
    filter_clause = ""
    if has_table(con, "category_dim") and has_column(con, "category_dim", "category") and has_column(con, "category_dim", "is_transfer"):
        filter_clause = "WHERE COALESCE(cd.is_transfer, FALSE) = FALSE"

    con.execute(f"""
        CREATE OR REPLACE TABLE monthly_actuals_by_category AS
        WITH t AS (
          SELECT
            strftime('%Y-%m', twc.date) AS month,
            COALESCE(twc.category, 'Uncategorized') AS category,
            CAST(twc.amount_cents/100.0 AS DOUBLE) AS amt
          FROM transactions_with_category twc
          LEFT JOIN category_dim cd ON cd.category = twc.category
          {filter_clause}
        )
        SELECT
          month,
          category,
          SUM(amt)                                   AS actual_signed,
          SUM(CASE WHEN amt < 0 THEN -amt ELSE 0 END) AS spending
        FROM t
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    print("Built monthly_actuals_by_category")

    if has_table(con, "category_enriched"):
        lvl_cols = [c for c in ("level1", "level2", "level3", "parent", "group_1", "group_2") if has_column(con, "category_dim", c)]
        select_lvls = ", ".join([f"ce.{c}" for c in lvl_cols]) if lvl_cols else ""
        comma = ", " if select_lvls else ""
        group_lvls = ("," + ",".join([f"ce.{c}" for c in lvl_cols])) if lvl_cols else ""

        filter_clause = ""
        if has_table(con, "category_dim") and has_column(con, "category_dim", "category") and has_column(con, "category_dim", "is_transfer"):
            filter_clause = "WHERE COALESCE(cd.is_transfer, FALSE) = FALSE"

        con.execute(f"""
            CREATE OR REPLACE TABLE monthly_actuals_by_category_enriched AS
            WITH t AS (
              SELECT
                strftime('%Y-%m', twc.date) AS month,
                COALESCE(twc.category, 'Uncategorized') AS category,
                CAST(twc.amount_cents/100.0 AS DOUBLE) AS amt
              FROM transactions_with_category twc
              LEFT JOIN category_dim cd ON cd.category = twc.category
              {filter_clause}
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
# CALENDAR / DATE DIM (single source of truth)
# -------------------------
fiscal_start = int(os.getenv("FISCAL_YEAR_START_MONTH", "1"))
if fiscal_start < 1 or fiscal_start > 12:
    fiscal_start = 1  # safety
FS = fiscal_start  # for f-strings

def _minmax(sql):
    row = con.execute(sql).fetchone()
    return row[0] if row and row[0] is not None else None

min_date = _minmax("""
    SELECT MIN(d) FROM (
      SELECT MIN(date)           AS d FROM transactions
      UNION ALL SELECT MIN(month)        FROM budget_monthly
      UNION ALL SELECT MIN(as_of_date)   FROM balance_snapshot
      UNION ALL SELECT MIN(as_of_date)   FROM positions
    )
""")
max_date = _minmax("""
    SELECT MAX(d) FROM (
      SELECT MAX(date)           AS d FROM transactions
      UNION ALL SELECT MAX(month)        FROM budget_monthly
      UNION ALL SELECT MAX(as_of_date)   FROM balance_snapshot
      UNION ALL SELECT MAX(as_of_date)   FROM positions
    )
""")

def build_calendar(series_sql: str):
    con.execute(f"""
        CREATE OR REPLACE TABLE calendar_dim AS
        SELECT
          gs AS date,
          CAST(EXTRACT(year  FROM gs) AS INT)                         AS year,
          CAST(EXTRACT(month FROM gs) AS INT)                         AS month_num,
          CAST(EXTRACT(day   FROM gs) AS INT)                         AS day_num,
          strftime('%Y-%m', gs)                                       AS year_month,
          CAST(strftime('%Y%m', gs) AS INT)                           AS yyyymm,
          date_trunc('month', gs)::DATE                               AS month_start,
          (date_trunc('month', gs) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE AS month_end,
          EXTRACT(quarter FROM gs)::INT                               AS quarter_num,
          date_trunc('quarter', gs)::DATE                             AS quarter_start,
          CASE WHEN strftime('%w', gs) IN ('0','6') THEN TRUE ELSE FALSE END AS is_weekend,
          CAST(strftime('%V', gs) AS INT)                             AS iso_week,
          CAST(strftime('%G', gs) AS INT)                             AS iso_year,
          date_trunc('week', gs)::DATE                                AS week_start_monday,
          strftime('%b', gs)                                          AS month_short,
          strftime('%B', gs)                                          AS month_name,
          'Q' || CAST(EXTRACT(quarter FROM gs) AS INT)                AS quarter_label,
          (gs = (date_trunc('month', gs) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) AS is_eom,
          CAST(EXTRACT(day FROM (date_trunc('month', gs) + INTERVAL 1 MONTH - INTERVAL 1 DAY)) AS INT) AS days_in_month,
          -- Fiscal fields (year labeled to FY end year)
          CASE WHEN CAST(EXTRACT(month FROM gs) AS INT) >= {FS}
                 THEN CAST(EXTRACT(year FROM gs) AS INT)
               ELSE CAST(EXTRACT(year FROM gs) AS INT) - 1
          END                                                         AS fiscal_year,
          CAST(CEIL((((((CAST(EXTRACT(month FROM gs) AS INT) - {FS} + 12) % 12) + 1)) / 3.0)) AS INT) AS fiscal_quarter,
          CAST(((CAST(EXTRACT(month FROM gs) AS INT) - {FS} + 12) % 12) + 1 AS INT)           AS fiscal_month_num
        FROM ({series_sql}) AS t(gs)
        ORDER BY 1
    """)

if min_date is None or max_date is None:
    # Build +/- 365 days around today
    series = "SELECT * FROM generate_series(CURRENT_DATE - INTERVAL 365 DAY, CURRENT_DATE + INTERVAL 365 DAY, INTERVAL 1 DAY)"
    build_calendar(series)
else:
    # Inline the literals so no separate CTE is required
    series = f"SELECT * FROM generate_series(DATE '{min_date}', DATE '{max_date}', INTERVAL 1 DAY)"
    build_calendar(series)

print("Built calendar_dim")

# Rebuild month_dim from calendar
con.execute("""
    CREATE OR REPLACE TABLE month_dim AS
    SELECT DISTINCT month_start AS month
    FROM calendar_dim
    ORDER BY 1
""")
print("Rebuilt month_dim from calendar_dim")


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

safe_copy("calendar_dim", "calendar_dim.parquet")

print("Done.")
