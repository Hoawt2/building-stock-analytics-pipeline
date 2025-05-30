import yfinance as yf
import os
import logging
import yfinance.shared as shared
from utils_stock import (
    get_existing_time_ids,
    get_all_tickers,
    fetch_missing_data_for_dates,
    update_fact_stock_history_batch
)
from datetime import datetime

# Gán proxy toàn cục cho yfinance (proxy HTTPS)
shared._proxy = "https://brd-customer-hl_72b2ff55-zone-scraping_browser2:i53mekuupgbb@brd.superproxy.io:9515"

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def update_stock_history_for_all_stocks():
    tickers = get_all_tickers()
    existing_time_ids = set(get_existing_time_ids())  # Dùng set để tra cứu nhanh O(1)

    start_date = os.getenv("START_DATE", "2025-01-01")
    end_date = datetime.now().strftime('%Y-%m-%d')

    for ticker in tickers:
        logging.info(f"🔄 Đang cập nhật dữ liệu lịch sử cho: {ticker} ({start_date} → {end_date})")

        try:
            missing_data = fetch_missing_data_for_dates(ticker, start_date, end_date, existing_time_ids)
        except Exception as e:
            logging.error(f"❌ Lỗi khi lấy dữ liệu cho {ticker}: {e}")
            continue

        if missing_data:
            try:
                update_fact_stock_history_batch(missing_data)
                logging.info(f"✅ Đã cập nhật {len(missing_data)} bản ghi cho {ticker}")
            except Exception as e:
                logging.error(f"❌ Lỗi khi ghi dữ liệu {ticker} vào DB: {e}")
        else:
            logging.info(f"ℹ️ Không có dữ liệu mới cho {ticker}")
