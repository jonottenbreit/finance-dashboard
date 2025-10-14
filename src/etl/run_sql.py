# run_sql.py
import sys, duckdb, pathlib

DB = r"C:\Users\jo136\OneDrive\FinanceData\finance.duckdb"

def main():
    if len(sys.argv) < 2:
        print("Usage:\n  python run_sql.py \"SELECT ...;\"\n  python run_sql.py --file path\\to\\query.sql")
        sys.exit(1)

    if sys.argv[1] == "--file":
        if len(sys.argv) < 3:
            sys.exit("ERROR: --file requires a path")
        sql_path = pathlib.Path(sys.argv[2])
        sql = sql_path.read_text(encoding="utf-8")
    else:
        # Join all args so you can include spaces and semicolons
        sql = " ".join(sys.argv[1:])

    con = duckdb.connect(DB)
    try:
        df = con.execute(sql).fetchdf()
        # Pretty print; avoids PowerShell escaping headaches
        try:
            print(df.to_string(index=False))
        except Exception:
            print(df)
    finally:
        con.close()

if __name__ == "__main__":
    main()
