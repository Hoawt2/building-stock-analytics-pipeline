import yfinance as yf
from db_connect import connect_db
from datetime import datetime, timedelta

def get_db_connection():
    conn = connect_db()
    return conn

def close_db_connection(cursor, conn):
    cursor.close()
    conn.close()

def get_existing_index_time_ids():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT time_id FROM fact_index_history;")
    time_ids = [row[0] for row in cursor.fetchall()]
    close_db_connection(cursor, conn)
    return time_ids

def get_all_index_tickers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM dim_index;")
    tickers = [row[0] for row in cursor.fetchall()]
    close_db_connection(cursor, conn)
    return tickers

def get_index_id_by_ticker(ticker):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT index_id FROM dim_index WHERE ticker = %s", (ticker,))
    result = cursor.fetchone()
    close_db_connection(cursor, conn)
    return result[0] if result else None

def get_date_str_from_relative_date(relative_date):
    if relative_date == 'yesterday':
        return (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif relative_date == '5 years ago':
        return (datetime.today() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    else:
        raise ValueError(f"Ngày không hợp lệ: {relative_date}")

def fetch_index_missing_data_for_dates(ticker, start_date, end_date, existing_time_ids):
    start_date_str = get_date_str_from_relative_date(start_date) if start_date in ["yesterday", "5 years ago"] else start_date
    end_date_str = get_date_str_from_relative_date(end_date) if end_date in ["yesterday", "5 years ago"] else end_date

    index_data = yf.Ticker(ticker).history(start=start_date_str, end=end_date_str)

    missing_data = []
    for index, row in index_data.iterrows():
        time_id = int(index.strftime('%Y%m%d'))
        if time_id not in existing_time_ids:
            open_val = row['Open']
            high_val = row['High']
            low_val = row['Low']
            close_val = row['Close']
            last_val = close_val
            daily_return = ((close_val - open_val) / open_val) if open_val else None
            market_volatility = (high_val - low_val) if high_val and low_val else None

            missing_data.append({
                "index_id": get_index_id_by_ticker(ticker),
                "time_id": time_id,
                "open_value": open_val,
                "high_value": high_val,
                "low_value": low_val,
                "last_value": last_val,
                "daily_return": daily_return,
                "market_volatility": market_volatility
            })

    return missing_data

def update_fact_index_history_batch(missing_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO fact_index_history (
        index_id, time_id, open_value, high_value, low_value,
        `last_value`, daily_return, market_volatility
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, [(d["index_id"], d["time_id"], d["open_value"], d["high_value"],
                                       d["low_value"], d["last_value"], d["daily_return"], d["market_volatility"])
                                      for d in missing_data])
    conn.commit()
    close_db_connection(cursor, conn)
