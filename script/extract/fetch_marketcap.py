import os
import argparse
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from datetime import datetime
from time import sleep

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
            query = text("SELECT MAX(date) FROM FMP_company_market_cap WHERE symbol = :symbol")
            result = connection.execute(query, {"symbol": symbol}).scalar()
            return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"Lỗi khi truy vẫn ngày gần nhất cho mã {symbol}: {e}")
        return None

# ============================================================ 
# 3️⃣ GỌI API FINANCIAL MODELING PREP
# ============================================================ 
def fetch_marketcap_from_api(symbol, api_key):
    url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?apikey={api_key}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"[{symbol}] Lỗi: Không thể giải mã phản hồi JSON từ API.")
            print(f"Nội dung phản hồi: {response.text}")
            return []

        if isinstance(data, dict) and "Error Message" in data:
            print(f"[{symbol}] Lỗi từ API: {data['Error Message']}")
            return []
        
        return data

    except requests.exceptions.Timeout:
        print(f"[{symbol}] Lỗi: Hết thời gian chờ khi gọi API.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Lỗi kết nối đến API: {e}")
        return []
    except Exception as e:
        print(f"[{symbol}] Lỗi không xác định đã xảy ra: {e}")
        return []

# ============================================================ 
# 4️⃣ CHUYỂN ĐỔI DỮ LIỆU
# ============================================================ 
def transform_marketcap_data(raw_data, symbol):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    if df.empty:
        return pd.DataFrame()

    df['symbol'] = symbol
    
    column_mapping = {
        'marketCap': 'market_cap'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    df['date'] = pd.to_datetime(df['date'])
    
    numeric_cols = ['market_cap']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    df = df.replace({np.nan: None})
    
    df.drop_duplicates(subset=['symbol', 'date'], keep='first', inplace=True)
    
    ordered_cols = ["symbol", "date", "market_cap"]
    df = df[[c for c in ordered_cols if c in df.columns]]
    
    return df

# ============================================================ 
# 5️⃣ GHI DỮ LIỆU VÀO DATABASE
# ============================================================ 
def load_marketcap_to_db(engine, df, symbol):
    print(f"[{symbol}] Chuẩn bị ghi {len(df)} bản ghi vào DB.", flush=True)
    if df.empty:
        print(f"[{symbol}] Không có dữ liệu để ghi.", flush=True)
        return
    
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    with engine.connect() as connection:
        metadata = MetaData()
        table = Table('FMP_company_market_cap', metadata, autoload_with=engine)
        data_to_insert = df.to_dict(orient='records')

        insert_stmt = mysql_insert(table).values(data_to_insert)

        update_columns = {
            'market_cap': insert_stmt.inserted['market_cap']
        }

        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
        
        transaction = connection.begin()
        try:
            connection.execute(upsert_stmt)
            transaction.commit()
            
            query = text("SELECT COUNT(*) FROM FMP_company_market_cap WHERE symbol = :symbol")
            count = connection.execute(query, {"symbol": symbol}).scalar()
            print(f"[{symbol}] ✅ Đã ghi thành công và/hoặc cập nhật {len(data_to_insert)} bản ghi. Tổng số bản ghi cho mã này: {count}.", flush=True)
            
        except Exception as e:
            print(f"[{symbol}] ❌ Lỗi khi ghi dữ liệu vào DB: {e}", flush=True)
            transaction.rollback()

# ============================================================ 
# 6️⃣ HÀM MAIN
# ============================================================ 
def fetch_marketcap_data(mode='daily'):
    load_dotenv(".env")
    api_key = os.getenv("FINANCIAL_MODELING_PREP_API_KEY")
    if not api_key:
        raise ValueError("API key cho Financial Modeling Prep không được tìm thấy trong file .env")
    
    engine = get_db_engine()
    stock_list_path = os.path.join(os.path.dirname(__file__), "..", "..", "stock_list.csv")
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return
    
    print(f"Bắt đầu fetch dữ liệu vốn hóa thị trường cho các mã: {tickers}")
    
    for symbol in tickers:
        print(f"=== [{symbol}] BẮT ĐẦU ===")
        raw_data = fetch_marketcap_from_api(symbol, api_key)
        if not raw_data:
            print(f"[{symbol}] Không có dữ liệu vốn hóa. Bỏ qua.")
            continue
        
        transformed_df = transform_marketcap_data(raw_data, symbol)
        
        if mode == "daily":
            max_date = get_max_date(symbol, engine)
            if max_date is not None:
                transformed_df = transformed_df[transformed_df["date"] > max_date]
                print(f"[{symbol}] daily: giữ lại dữ liệu mới hơn {max_date.date()}. Số bản ghi sau lọc: {len(transformed_df)}")

        if transformed_df.empty:
            print(f"[{symbol}] ❗ Sau khi lọc không còn bản ghi để ghi. Bỏ qua.")
        else:
            load_marketcap_to_db(engine, transformed_df, symbol)

        # API của FMP có giới hạn, nên có một khoảng nghỉ nhỏ
        sleep(1)

    print("\n✅ Hoàn tất quá trình fetch vốn hóa thị trường.")

# ============================================================ 
# 7️⃣ CHẠY TỪ DÒNG LỆNH
# ============================================================ 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch market capitalization data from FMP API.")
    parser.add_argument(
        "--mode",
        type=str,
        default="daily",
        choices=["daily", "historical"],
        help="Chế độ fetch: 'daily' chỉ lấy dữ liệu mới hơn trong DB, 'historical' lấy toàn bộ lịch sử."
    )
    args = parser.parse_args()

    fetch_marketcap_data(mode=args.mode)
