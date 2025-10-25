import os
import argparse
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore")

# ==========================================================
# 1️⃣ Load environment variables
# ==========================================================
load_dotenv(".env")

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = "localhost"
MYSQL_PORT = 3307
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
START_DATE = os.getenv("START_DATE", "2015-01-01")

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# ==========================================================
# 2️⃣ Fetch data from yfinance
# ==========================================================
def fetch_data(symbol, start_date, end_date):
    print(f"📈 Fetching {symbol} from {start_date} to {end_date}")
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)
        print(f"📦 Fetched {len(df)} rows for {symbol}")
    except Exception as e:
        print(f"🚨 Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠️ No data for {symbol}")
        return pd.DataFrame()

    df.reset_index(inplace=True)
    df["Symbol"] = symbol.strip()
    df.rename(
        columns={
            "Date": "date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Volume": "volume",
        },
        inplace=True,
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    

    return df[["Symbol", "date", "open_price", "high_price", "low_price", "close_price", "volume"]]

# ==========================================================
# 3️⃣ Save to MySQL with better logging
# ==========================================================
def save_to_mysql(df, engine):
    if df.empty:
        print("⚠️ No data to insert.")
        return

    success_count = 0
    fail_count = 0

    with engine.begin() as conn:
        for i, row in df.iterrows():
            try:
                symbol = str(row["Symbol"]).strip()
                query = text("""
                    INSERT INTO raw_yfinance 
                    (symbol, date, open_price, high_price, low_price, close_price, volume)
                    VALUES (:symbol, :date, :open_price, :high_price, :low_price, :close_price, :volume)
                    ON DUPLICATE KEY UPDATE
                        open_price = VALUES(open_price),
                        high_price = VALUES(high_price),
                        low_price = VALUES(low_price),
                        close_price = VALUES(close_price),
                        volume = VALUES(volume),
                        load_timestamp = CURRENT_TIMESTAMP
                """)
                conn.execute(query, {
                    "symbol": symbol,
                    "date": pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),
                    "open_price": float(row["open_price"]),
                    "high_price": float(row["high_price"]),
                    "low_price": float(row["low_price"]),
                    "close_price": float(row["close_price"]),
                    "volume": int(row["volume"]),
                })
                success_count += 1
            except Exception as e:
                print(f"❌ Error inserting row {i} for {row['Symbol']}: {e}")
                fail_count += 1

    print(f"✅ Inserted/Updated {success_count} rows | ❌ Failed: {fail_count}")

# ==========================================================
# 4️⃣ Get last available date for symbol
# ==========================================================
def get_last_date(symbol, engine):
    query = text("SELECT MAX(date) FROM raw_yfinance WHERE symbol = :symbol")
    with engine.connect() as conn:
        result = conn.execute(query, {"symbol": symbol}).scalar()
    return result

# ==========================================================
# 5️⃣ Main logic
# ==========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock data from yfinance and load to MySQL.")
    parser.add_argument("--mode", choices=["historical", "daily"], required=True)
    parser.add_argument("--stock_list", default="stock_list.csv", help="Path to CSV file containing stock symbols.")
    args = parser.parse_args()

    symbols = pd.read_csv(args.stock_list)["symbol"].astype(str).str.strip().tolist()
    today = datetime.today()

    for symbol in symbols:
        if args.mode == "historical":
            start_date = START_DATE
        else:
            last_date = get_last_date(symbol, engine)
            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = START_DATE

        end_date = today.strftime("%Y-%m-%d")

        if datetime.strptime(start_date, "%Y-%m-%d") > today:
            print(f"✅ {symbol} is already up to date.")
            continue

        df = fetch_data(symbol, start_date, end_date)
        if not df.empty:
            save_to_mysql(df, engine)
        else:
            print(f"⚠️ No new data for {symbol}.")

    print("🏁 Completed fetching and saving data.")
