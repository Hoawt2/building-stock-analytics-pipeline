import yfinance as yf
import pandas as pd
from datetime import datetime

# Ticker
ticker = yf.Ticker("AAPL")

# Tải dữ liệu
financials = ticker.quarterly_financials
balance_sheet = ticker.quarterly_balance_sheet
cashflow = ticker.quarterly_cashflow
currency = ticker.info.get('currency', 'USD')  # Lấy đơn vị tiền tệ

# Chuyển cột thành datetime
for df in [financials, balance_sheet, cashflow]:
    df.columns = pd.to_datetime(df.columns)

# Định nghĩa mốc thời gian
start_date = pd.Timestamp("2020-01-01")
today = pd.Timestamp(datetime.today().date())

# Lọc các cột nằm từ 2020 trở đi
available_dates = [date for date in financials.columns if start_date <= date <= today]

# Tạo DataFrame tổng hợp
data = []

for date in available_dates:
    fiscal_year = date.year
    fiscal_quarter = (date.month - 1) // 3 + 1  # Tính quý

    row = {
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "revenue": financials.at["Total Revenue", date] if "Total Revenue" in financials.index else None,
        "net_income": financials.at["Net Income", date] if "Net Income" in financials.index else None,
        "total_assets": balance_sheet.at["Total Assets", date] if "Total Assets" in balance_sheet.index else None,
        "total_liabilities": balance_sheet.at["Total Liabilities Net Minority Interest", date] if "Total Liabilities Net Minority Interest" in balance_sheet.index else None,
        "operating_cash_flow": cashflow.at["Total Cash From Operating Activities", date] if "Total Cash From Operating Activities" in cashflow.index else None,
        "eps": financials.at["Diluted EPS", date] if "Diluted EPS" in financials.index else None,
        "currency": currency,
    }
    data.append(row)

# Kết quả
result_df = pd.DataFrame(data)

# Hiển thị
print(result_df)
