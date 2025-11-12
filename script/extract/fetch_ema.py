import os
import argparse
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from time import sleep
from sqlalchemy.dialects.mysql import insert as mysql_insert

# ============================================================
# 1️⃣ TẠO ENGINE KẾT NỐI DATABASE
# ============================================================
def get_db_engine():
    load_dotenv(".env")
    
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = os.getenv("MYSQL_DATABASE")
    mysql_host = "localhost"
    mysql_port = "3307"
    
    if not all([mysql_user, mysql_password, mysql_db, mysql_host, mysql_port]):
        raise ValueError("Thiếu hoặc sai thông tin kết nối database trong file .env")
    
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
    engine = create_engine(connection_string)
    return engine 

# ============================================================
# 2️⃣ HÀM LẤY NGÀY GẦN NHẤT TRONG DB
# ============================================================
def get_max_date(symbol, engine):
    try:
        with engine.connect() as connection:
            query = text("SELECT MAX(date) FROM alphavantage_EMA WHERE symbol = :symbol")
            result = connection.execute(query, {"symbol": symbol}).scalar()
            return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"Lỗi khi truy vẫn ngày gần nhất cho mã {symbol}: {e}")
        return None

# ============================================================
# 3️⃣ GỌI API ALPHAVANTAGE
# ============================================================
def fetch_ema_from_api(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=EMA&symbol={symbol}&interval=daily&time_period=50&series_type=open&apikey={api_key}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            print(f"[{symbol}] Lỗi từ API: {data['Error Message']}")
            return None
        if "Information" in data:
            print(f"[{symbol}] Thông tin từ API: {data['Information']}")
            return None
        
        return data.get("Technical Analysis: EMA")

    except requests.exceptions.Timeout:
        print(f"[{symbol}] Lỗi: Hết thời gian chờ khi gọi API.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Lỗi kết nối đến API: {e}")
        return None
    except Exception as e:
        print(f"[{symbol}] Lỗi không xác định đã xảy ra: {e}")
        return None

# ============================================================
# 4️⃣ CHUYỂN ĐỔI DỮ LIỆU
# ============================================================
def transform_ema_data(raw_data, symbol):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame.from_dict(raw_data, orient='index')
    if df.empty:
        return pd.DataFrame()

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date', 'EMA': 'ema'}, inplace=True)
    df['symbol'] = symbol

    df['date'] = pd.to_datetime(df['date'])
    df['ema'] = pd.to_numeric(df['ema'], errors='coerce')

    df.replace({np.nan: None}, inplace=True)
    
    df.drop_duplicates(subset=['symbol', 'date'], keep='first', inplace=True)
    
    ordered_cols = ['symbol', 'date', 'ema']
    df = df[ordered_cols]
    
    return df

# ============================================================
# 5️⃣ GHI DỮ LIỆU VÀO DATABASE
# ============================================================
def load_ema_to_db(engine, df, symbol):
    print(f"[{symbol}] Chuẩn bị ghi {len(df)} bản ghi vào DB.", flush=True)
    if df.empty:
        print(f"[{symbol}] Không có dữ liệu để ghi.", flush=True)
        return
    
    with engine.connect() as connection:
        metadata = MetaData()
        table = Table('alphavantage_EMA', metadata, autoload_with=engine)
        data_to_insert = df.to_dict(orient='records')

        insert_stmt = mysql_insert(table).values(data_to_insert)

        update_columns = {
            col: insert_stmt.inserted[col] for col in df.columns if col not in ['symbol', 'date']
        }

        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
        
        transaction = connection.begin()
        try:
            connection.execute(upsert_stmt)
            transaction.commit()
            
            query = text("SELECT COUNT(*) FROM alphavantage_EMA WHERE symbol = :symbol")
            count = connection.execute(query, {"symbol": symbol}).scalar()
            print(f"[{symbol}] ✅ Đã ghi thành công và/hoặc cập nhật {len(data_to_insert)} bản ghi. Tổng số bản ghi cho mã này: {count}.", flush=True)
            
        except Exception as e:
            print(f"[{symbol}] ❌ Lỗi khi ghi dữ liệu vào DB: {type(e).__name__} - {e}", flush=True)
            # In ra một phần của dữ liệu gây lỗi để dễ debug
            if data_to_insert:
                print(f"[{symbol}] Dữ liệu gây lỗi (5 bản ghi đầu): {data_to_insert[:5]}", flush=True)
            transaction.rollback()

# ============================================================
# 6️⃣ HÀM MAIN
# ============================================================
def fetch_ema_data(mode='daily'):
    load_dotenv(".env")
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("API key cho AlphaVantage không được tìm thấy trong file .env")
    start_date_historical = os.getenv("START_DATE", "2015-01-01")
    start_date_historical = pd.to_datetime(start_date_historical)
    
    engine = get_db_engine()
    stock_list_path = os.path.join(os.path.dirname(__file__), "..", "..", "stock_list.csv")
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return
    
    print(f"Bắt đầu fetch dữ liệu EMA cho các mã: {tickers}")
    
    for symbol in tickers:
        print(f"=== [{symbol}] BẮT ĐẦU ===")
        raw_data = fetch_ema_from_api(symbol, api_key)
        
        if not raw_data:
            print(f"[{symbol}] Không có dữ liệu. Bỏ qua.")
            continue

        df = transform_ema_data(raw_data, symbol)

        if mode == "historical":
            df = df[df["date"] >= start_date_historical]
            print(f"[{symbol}] historical: giữ lại dữ liệu từ {start_date_historical.date()} trở về sau. Số bản ghi sau lọc: {len(df)}")
        else: # mode == "daily"
            max_date = get_max_date(symbol, engine)
            if max_date is not None:
                df = df[df["date"] > max_date]
                print(f"[{symbol}] daily: giữ lại dữ liệu mới hơn {max_date.date()}. Số bản ghi sau lọc: {len(df)}")

        if df.empty:
            print(f"[{symbol}] ❗ Sau khi lọc không còn bản ghi để ghi. Bỏ qua.")
        else:
            load_ema_to_db(engine, df, symbol)
        
        # Alpha Vantage API: 5 requests/phút -> nghỉ 15s/mã
        sleep(15)

    print("✅ Hoàn tất quá trình fetch EMA.")

# ============================================================
# 7️⃣ CHẠY TỪ DÒNG LỆNH
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch EMA data from Alpha Vantage API.")
    parser.add_argument(
        "--mode",
        type=str,
        default="daily",
        choices=["daily", "historical"],
        help="Chế độ fetch: 'daily' chỉ lấy dữ liệu mới hơn trong DB, 'historical' lấy toàn bộ lịch sử."
    )
    args = parser.parse_args()

    fetch_ema_data(mode=args.mode)