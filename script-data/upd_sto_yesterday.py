from datetime import datetime, timedelta
from db_connect import connect_db
from utils_stock import get_existing_time_ids, get_all_tickers, fetch_missing_data_for_dates, update_fact_stock_history_batch

def update_yesterday():
    tickers = get_all_tickers()
    
    existing_time_ids = get_existing_time_ids()

    yesterday = (datetime.now()).strftime('%Y-%m-%d')

    for ticker in tickers:
        print(f"Đang cập nhật dữ liệu ngày hôm qua cho: {ticker}")
        
        missing_data = fetch_missing_data_for_dates(ticker, yesterday, yesterday, existing_time_ids)
        
        if missing_data:
            update_fact_stock_history_batch(missing_data)
            print(f"✅ Đã cập nhật dữ liệu cho {ticker}")
        else:
            print(f"ℹ️ Không có dữ liệu mới cho {ticker}")

