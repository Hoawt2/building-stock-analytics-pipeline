import pandas as pd
import yfinance as yf
import requests
from db_connect import connect_db
from datetime import datetime, timedelta
from config import API_KEY_FMP

FMP_URL_TEMPLATE = "https://financialmodelingprep.com/api/v3/historical-market-capitalization/{ticker}"

def get_db_connection():
    conn = connect_db()
    return conn

def close_db_connection(cursor, conn):
    cursor.close()
    conn.close()

def get_existing_time_ids():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT time_id FROM fact_stock_history;")
    time_ids = [row[0] for row in cursor.fetchall()]
    close_db_connection(cursor, conn)
    return time_ids

def get_all_tickers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM dim_stock;")
    tickers = [row[0] for row in cursor.fetchall()]
    close_db_connection(cursor, conn)
    return tickers

def get_date_str_from_relative_date(relative_date):
    if relative_date == 'yesterday':
        return (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif relative_date == '5 years ago':
        return (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    else:
        raise ValueError(f"Ngày không hợp lệ: {relative_date}")

def get_market_cap_for_ticker(ticker, start_date, end_date):
    url = FMP_URL_TEMPLATE.format(ticker=ticker)
    params = {
        "from": start_date,
        "to": end_date,
        "apikey": API_KEY_FMP
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if len(data) > 0:
            return {item['date']: item['marketCap'] for item in data}
    return {}

def fetch_market_cap_by_year(ticker, start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    current_date = datetime.today()
    market_cap_dict = {}

    while start <= end and start <= current_date:
        year_end = min(start.replace(year=start.year + 1, day=1) - timedelta(days=1), end, current_date)
        chunk_start_str = start.strftime('%Y-%m-%d')
        chunk_end_str = year_end.strftime('%Y-%m-%d')
        
        chunk_data = get_market_cap_for_ticker(ticker, chunk_start_str, chunk_end_str)
        market_cap_dict.update(chunk_data)
        
        start = year_end + timedelta(days=1)

    return market_cap_dict

def fetch_missing_data_for_dates(ticker, start_date, end_date, existing_time_ids):
    if start_date not in ["yesterday", "5 years ago"]:
        start_date_str = start_date
    else:
        start_date_str = get_date_str_from_relative_date(start_date)

    if end_date not in ["yesterday", "5 years ago"]:
        end_date_str = end_date
    else:
        end_date_str = get_date_str_from_relative_date(end_date)

    stock = yf.Ticker(ticker)
    historical_data = stock.history(start=start_date_str, end=end_date_str)

    market_cap_dict = fetch_market_cap_by_year(ticker, start_date_str, end_date_str)

    missing_data = []
    for index, row in historical_data.iterrows():
        time_id = int(row.name.strftime('%Y%m%d'))
        date_str = row.name.strftime('%Y-%m-%d')

        if time_id not in existing_time_ids:

            if date_str not in market_cap_dict:
                pass

            missing_data.append({
                "stock_id": get_stock_id_by_ticker(ticker),
                "time_id": time_id,
                "open_price": row['Open'],
                "high_price": row['High'],
                "low_price": row['Low'],
                "last_price": row['Close'],
                "close_price": row['Close'],
                "volume": row['Volume'],
                "market_cap": market_cap_dict.get(date_str)
            })

    return missing_data

def get_stock_id_by_ticker(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT stock_id FROM dim_stock WHERE ticker = %s", (ticker,))
    stock_id = cursor.fetchone()[0]
    close_db_connection(cursor, conn)
    return stock_id

def update_fact_stock_history_batch(missing_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO fact_stock_history (stock_id, time_id, open_price, high_price, low_price, last_price, close_price, volume, market_cap)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, [(data["stock_id"], data["time_id"], data["open_price"], data["high_price"],
                                       data["low_price"], data["last_price"], data["close_price"],
                                       data["volume"], data["market_cap"])
                                      for data in missing_data])
    conn.commit()
    close_db_connection(cursor, conn)
