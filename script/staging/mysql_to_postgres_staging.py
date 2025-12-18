import os
import re
import pandas as pd
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
DOTENV_PATH = '/opt/airflow/.env'

TABLE_CONFIG = {
    'raw_yfinance': 'date',
    'alphavantage_cash_flow': 'fiscal_date_ending',
    'alphavantage_balance_sheet': 'fiscal_date_ending',
    'alphavantage_income_statement': 'fiscal_date_ending',
    'alphavantage_earnings': 'fiscal_date_ending',
    'fmp_company_market_cap': 'date',
    'fmp_company_information': 'id',
}
SQL_DEFINITIONS_FILE = '/opt/airflow/sql/extract_db.sql'

def get_db_engines():
    """Connects to MySQL and PostgreSQL using settings from .env file."""
    DOTENV_PATH = '/opt/airflow/.env'
    load_dotenv(DOTENV_PATH)
    try:
        mysql_engine = create_engine(f'mysql+pymysql://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@{os.getenv("MYSQL_HOST")}:{os.getenv("MYSQL_PORT")}/{os.getenv("MYSQL_DATABASE")}')
        pg_engine = create_engine(
            f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}',
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

    # Mở kết nối MySQL một lần để dùng cho vòng lặp (hoặc mở trong từng vòng lặp tùy ý)
    # Tốt nhất là mở trong vòng lặp để đảm bảo clean session
    for table in mysql_tables:
        pg_table = f"stg_{table}"
        print(f"\n--- Processing table: {table} ---")
        try:
            # --- SỬA ĐỔI QUAN TRỌNG Ở ĐÂY ---
            # Tạo kết nối rõ ràng từ engine
            with mysql_engine.connect() as mysql_conn:
                
                # HISTORICAL MODE: Always do a full replace.
                if args.mode == 'historical'or table not in TABLE_CONFIG:
                    print(f"Historical mode: Replacing all data in {pg_table}.")
                    df = pd.read_sql_table(table, mysql_conn)
                    if not df.empty:
                        # Bước 2: Dùng giao dịch (transaction) để xóa và nạp lại
                        with pg_engine.begin() as pg_conn:
                            print(f"Truncating table {pg_table}...")
                            pg_conn.execute(text(f'TRUNCATE TABLE "{pg_table}" CASCADE'))
            
                            # Dùng append để giữ nguyên cấu trúc BIGINT bạn đã tạo ở file init.sql
                            df.to_sql(pg_table, pg_conn, if_exists='append', index=False)
            
                        print(f"Successfully refreshed {len(df)} rows.")
                    else:
                        print(f"Source table {table} is empty. Skipping refresh.")
                
                
                else:
                    date_col = TABLE_CONFIG[table]
                    # Find the most recent date in the destination table
                    max_date = None
                    try:
                        with pg_engine.connect() as pg_conn:
                            max_date = pg_conn.execute(text(f'SELECT MAX("{date_col}") FROM "{pg_table}"')).scalar()
                    except Exception:
                        print(f"Info: Destination table {pg_table} likely doesn't exist. Will perform a full load.")

                    # Fetch only newer records from the source
                    if max_date:
                        print(f"Found latest date in destination: {max_date}. Fetching newer records.")
                        query = text(f"SELECT * FROM `{table}` WHERE `{date_col}` > :max_date")
                        # Sử dụng params an toàn hơn, hoặc format string như cũ nếu muốn
                        # Ở đây để đơn giản giữ nguyên logic format string của bạn nhưng cần cẩn thận SQL Injection
                        query_str = f"SELECT * FROM `{table}` WHERE `{date_col}` > '{max_date}'"
                    else:
                        print("No existing data found in destination. Fetching all records.")
                        query_str = f"SELECT * FROM `{table}`"
                    
                    # Thay mysql_engine bằng mysql_conn
                    df_new = pd.read_sql(query_str, mysql_conn)

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