import yfinance as yf
from utils_stock import get_existing_time_ids, get_all_tickers, fetch_missing_data_for_dates, update_fact_stock_history_batch
from datetime import datetime
import time
from yfinance.exceptions import YFRateLimitError

def update_stock_history_for_all_stocks():
    tickers = get_all_tickers()
    existing_time_ids = get_existing_time_ids()

    start_date = '2020-01-01'
    end_date = datetime.now().strftime('%Y-%m-%d')

    for ticker in tickers:
        print(f"Đang cập nhật dữ liệu lịch sử từ 01/01/2020 đến hôm nay cho: {ticker}")
        
        for attempt in range(3):  # Thử lại tối đa 3 lần nếu bị rate limit
            try:
                missing_data = fetch_missing_data_for_dates(ticker, start_date, end_date, existing_time_ids)

                if missing_data:
                    update_fact_stock_history_batch(missing_data)
                    print(f"✅ Đã cập nhật dữ liệu cho {ticker}")
                else:
                    print(f"ℹ️ Không có dữ liệu mới cho {ticker}")

                break  # Nếu thành công thì thoát retry loop

            except YFRateLimitError:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"⚠️ Bị giới hạn tốc độ khi lấy dữ liệu cho {ticker}, thử lại sau {wait_time} giây...")
                time.sleep(wait_time)
        
        # Luôn nghỉ 1-2 giây giữa các ticker
        time.sleep(2)
