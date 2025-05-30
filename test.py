import yfinance as yf
from datetime import datetime, timedelta

# Xác định ngày hôm qua
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# Tải dữ liệu AAPL hôm qua
ticker = yf.Ticker("AAPL")
data = ticker.history(start=yesterday_str, end=(yesterday + timedelta(days=1)).strftime('%Y-%m-%d'))

# Hiển thị dữ liệu hôm qua
if not data.empty:
    print(f"Giá AAPL hôm qua ({yesterday_str}):")
    print(data[['Open', 'High', 'Low', 'Close', 'Volume']])
else:
    print("Không có dữ liệu giao dịch cho hôm qua (có thể là ngày nghỉ lễ hoặc cuối tuần).")
