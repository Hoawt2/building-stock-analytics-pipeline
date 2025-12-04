import os
import re
import pandas as pd
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Define which tables can be updated incrementally and what date column to use.
# Tables not listed here will be fully replaced in each run.
TABLE_CONFIG = {
    'raw_yfinance': 'date',
    'alphavantage_cash_flow': 'fiscal_date_ending',
    'alphavantage_balance_sheet': 'fiscal_date_ending',
    'alphavantage_income_statement': 'fiscal_date_ending',
    'alphavantage_earnings': 'fiscal_date_ending',
    'fmp_company_market_cap': 'date',
}
SQL_DEFINITIONS_FILE = 'sql/extract_db.sql'
# --- END OF CONFIGURATION ---

def get_db_engines():
    """Connects to MySQL and PostgreSQL using settings from .env file."""
    load_dotenv(".env")
    try:
        mysql_engine = create_engine(f'mysql+pymysql://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@localhost:3307/{os.getenv("MYSQL_DATABASE")}')
        pg_engine = create_engine(
            f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/{os.getenv("POSTGRES_DB")}',
            connect_args={'options': '-csearch_path=staging'}
        )
        return mysql_engine, pg_engine
    except Exception as e:
        print(f"ERROR: Database connection failed. Check .env file. Details: {e}")
        return None, None

def get_table_names_from_sql(filepath):
    """Extracts table names from the CREATE TABLE statements in a .sql file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return re.findall(r"CREATE TABLE IF NOT EXISTS ?([a-zA-Z0-9_]+)?", content)
    except FileNotFoundError:
        print(f"ERROR: SQL file not found at: {filepath}")
        return []

def main():
    """Main function to transfer data from MySQL to PostgreSQL."""
    parser = argparse.ArgumentParser(description='MySQL to PostgreSQL Data Staging.')
    parser.add_argument('--mode', type=str, required=True, choices=['historical', 'incremental'])
    args = parser.parse_args()

    mysql_engine, pg_engine = get_db_engines()
    if not all([mysql_engine, pg_engine]): return

    mysql_tables = get_table_names_from_sql(SQL_DEFINITIONS_FILE)
    if not mysql_tables:
        print("No tables found to process. Exiting.")
        return

    print(f"Starting '{args.mode}' data transfer...")
    overall_success = True

    for table in mysql_tables:
        pg_table = f"stg_{table}"
        print(f"\n--- Processing table: {table} ---")
        try:
            # HISTORICAL MODE: Always do a full replace.
            if args.mode == 'historical':
                print(f"Historical mode: Replacing all data in {pg_table}.")
                df = pd.read_sql_table(table, mysql_engine)
                df.to_sql(pg_table, pg_engine, if_exists='replace', index=False)
                print(f"Successfully replaced {len(df)} rows.")
                continue

            # INCREMENTAL MODE:
            if table not in TABLE_CONFIG:
                print(f"Info: No incremental config for '{table}'. Performing full replacement.")
                df = pd.read_sql_table(table, mysql_engine)
                df.to_sql(pg_table, pg_engine, if_exists='replace', index=False)
                print(f"Successfully replaced {len(df)} rows.")
                continue

            date_col = TABLE_CONFIG[table]
            
            # Find the most recent date in the destination table
            max_date = None
            try:
                with pg_engine.connect() as conn:
                    max_date = conn.execute(text(f'SELECT MAX("{date_col}") FROM "{pg_table}"')).scalar()
            except Exception:
                print(f"Info: Destination table {pg_table} likely doesn't exist. Will perform a full load.")

            # Fetch only newer records from the source
            if max_date:
                print(f"Found latest date in destination: {max_date}. Fetching newer records.")
                query = f"SELECT * FROM `{table}` WHERE `{date_col}` > '{max_date}'"
            else:
                print("No existing data found in destination. Fetching all records.")
                query = f"SELECT * FROM `{table}`"
            
            df_new = pd.read_sql(query, mysql_engine)

            if not df_new.empty:
                print(f"Found {len(df_new)} new rows to append.")
                df_new.to_sql(pg_table, pg_engine, if_exists='append', index=False)
            else:
                print("No new data to append.")

        except Exception as e:
            print(f"ERROR processing {table}: {e}")
            overall_success = False
            continue
    
    status = "successfully" if overall_success else "with errors"
    print(f"\nData transfer process finished {status}.")
    if mysql_engine: mysql_engine.dispose()
    if pg_engine: pg_engine.dispose()

if __name__ == '__main__':
    main()