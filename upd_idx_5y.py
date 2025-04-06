from utils_index import (
    get_all_index_tickers,
    get_existing_index_time_ids,
    fetch_index_missing_data_for_dates,
    update_fact_index_history_batch
)
from datetime import datetime

def update_index_history_for_all_indices():
    tickers = get_all_index_tickers()
    existing_time_ids = get_existing_index_time_ids()
    start_date = '2020-01-01'
    end_date = (datetime.now()).strftime('%Y-%m-%d')

    for ticker in tickers:
        print(f"📈 Đang cập nhật lịch sử index cho: {ticker}")
        missing_data = fetch_index_missing_data_for_dates(ticker, start_date, end_date, existing_time_ids)

        if missing_data:
            update_fact_index_history_batch(missing_data)
            print(f"✅ Đã cập nhật xong cho {ticker}")
        else:
            print(f"ℹ️ Không có dữ liệu mới cho {ticker}")
