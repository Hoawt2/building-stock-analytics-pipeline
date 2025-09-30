import requests
from db_connect import connect_db

def fetch_fmp_key_metrics(ticker, api_key):
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?period=annual&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        return data if isinstance(data, list) else []
    except Exception as e:

        return []

def get_stock_list():

    conn = connect_db()
    if conn is None:

        return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT stock_id, ticker FROM dim_stock")
        stocks = cursor.fetchall()

        return stocks
    except Exception as e:

        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def get_time_id(date):

    conn = connect_db()
    if conn is None:

        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT date_key FROM dim_time WHERE full_date = %s", (date,))
        result = cursor.fetchone()

        return result[0] if result else None
    except Exception as e:

        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def check_existing_record(stock_id, time_id):

    conn = connect_db()
    if conn is None:

        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM fact_financial_ratios WHERE stock_id = %s AND time_id = %s", (stock_id, time_id))
        exists = cursor.fetchone() is not None

        return exists
    except Exception as e:

        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

def insert_financial_ratios(record):

    if not record['stock_id'] or not record['time_id']:

        return

    conn = connect_db()
    if conn is None:

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

    except Exception as e:

        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()