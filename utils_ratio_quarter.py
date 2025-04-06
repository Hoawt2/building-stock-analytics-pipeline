import requests
from db_connect import connect_db

def fetch_fmp_ratios_ttm(ticker, api_key):
    url = f"https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}?apikey={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}

def fetch_alpha_income_statement(ticker, api_key):
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}

def get_stock_list():
    conn = connect_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT stock_id, ticker FROM dim_stock")
    stocks = cursor.fetchall()
    cursor.close()
    conn.close()
    return stocks

def get_time_id(date):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT date_key FROM dim_time WHERE full_date = %s", (date,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None

def check_existing_record(stock_id, time_id):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM fact_financial_ratios WHERE stock_id = %s AND time_id = %s", (stock_id, time_id))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def insert_financial_ratios(record):
    conn = connect_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO fact_financial_ratios (
            stock_id, time_id, current_ratio, quick_ratio, roa, roe,
            debt_to_equity, pe_ratio, pb_ratio, dividend_yield,
            interest_coverage_ratio, ebitda_margin
        )
        VALUES (%(stock_id)s, %(time_id)s, %(current_ratio)s, %(quick_ratio)s, %(roa)s, %(roe)s,
                %(debt_to_equity)s, %(pe_ratio)s, %(pb_ratio)s, %(dividend_yield)s,
                %(interest_coverage_ratio)s, %(ebitda_margin)s)
    """
    cursor.execute(sql, record)
    conn.commit()
    cursor.close()
    conn.close()
