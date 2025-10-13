import yfinance as yf
import datetime

ticker = yf.Ticker("AAPL")
info = ticker.info

company_data = {
    "company_name": info.get("longName"),
    "sector": info.get("sector"),
    "industry": info.get("industry"),
    "country": info.get("country"),
    "ipo_year": datetime.datetime.fromtimestamp(
        info.get("firstTradeDateEpochUtc", 0)
    ).year if info.get("firstTradeDateEpochUtc") else None
}

print(company_data)
