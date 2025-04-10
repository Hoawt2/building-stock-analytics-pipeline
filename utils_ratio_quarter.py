import requests
from db_connect import connect_db

def fetch_fmp_key_metrics(ticker, api_key):
    print(f"[DEBUG] Fetching FMP key metrics for {ticker}")
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period=annual&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(f"[DEBUG] FMP response data for {ticker}: {data}")
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch FMP data for {ticker}: {e}")
        return []

def get_stock_list():
    print("[DEBUG] Fetching stock list from database")
    conn = connect_db()
    if conn is None:
        print("[ERROR] Database connection failed")
        return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT stock_id, ticker FROM dim_stock")
        stocks = cursor.fetchall()
        print(f"[DEBUG] Retrieved {len(stocks)} stocks: {stocks}")
        return stocks
    except Exception as e:
        print(f"[ERROR] Failed to fetch stock list: {e}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def get_time_id(date):
    print(f"[DEBUG] Fetching time_id for date: {date}")
    conn = connect_db()
    if conn is None:
        print("[ERROR] Database connection failed")
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT date_key FROM dim_time WHERE full_date = %s", (date,))
        result = cursor.fetchone()
        print(f"[DEBUG] time_id result for {date}: {result}")
        return result[0] if result else None
    except Exception as e:
        print(f"[ERROR] Failed to fetch time_id for {date}: {e}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def check_existing_record(stock_id, time_id):
    print(f"[DEBUG] Checking if record exists for stock_id={stock_id}, time_id={time_id}")
    conn = connect_db()
    if conn is None:
        print("[ERROR] Database connection failed")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM fact_financial_ratios WHERE stock_id = %s AND time_id = %s", (stock_id, time_id))
        exists = cursor.fetchone() is not None
        print(f"[DEBUG] Record exists: {exists}")
        return exists
    except Exception as e:
        print(f"[ERROR] Failed to check existing record: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def insert_financial_ratios(record):
    print(f"[DEBUG] Inserting financial ratios: {record}")
    if not record['stock_id'] or not record['time_id']:
        print(f"[ERROR] Missing stock_id or time_id: {record}")
        return

    conn = connect_db()
    if conn is None:
        print("[ERROR] Database connection failed")
        return
    cursor = None
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO fact_financial_ratios (
                stock_id, time_id, current_ratio, roe, debt_to_equity,
                pe_ratio, pb_ratio, dividend_yield, interest_coverage_ratio
            )
            VALUES (%(stock_id)s, %(time_id)s, %(current_ratio)s, %(roe)s, %(debt_to_equity)s,
                    %(pe_ratio)s, %(pb_ratio)s, %(dividend_yield)s, %(interest_coverage_ratio)s)
        """
        cursor.execute(sql, record)
        conn.commit()
        print(f"[DEBUG] Insert successful for stock_id={record['stock_id']}, time_id={record['time_id']}")
    except Exception as e:
        print(f"[ERROR] Failed to insert record for stock_id={record['stock_id']}, time_id={record['time_id']}: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()