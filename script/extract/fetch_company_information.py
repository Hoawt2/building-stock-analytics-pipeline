import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from time import sleep

DOTENV_PATH = '/opt/airflow/.env'
def get_db_engine():
    load_dotenv(DOTENV_PATH)
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = os.getenv("MYSQL_DATABASE")
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_port = os.getenv("MYSQL_PORT")
    
    if not all([mysql_user, mysql_password, mysql_db, mysql_host, mysql_port]):
        raise ValueError("Thiếu hoặc sai thông tin kết nối database trong file .env")
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
    engine = create_engine(connection_string)
    return engine

def fetch_company_info(symbol, api_key):
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={api_key}"
    try: 
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        try: 
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"[{symbol}] Lỗi: Không thể giải mã phản hồi JSON từ API.")
            print(f"Nội dung phản hồi: {response.text}")
            
        if "Error Message" in data:
            print(f"[{symbol}] Lỗi từ API: {data['Error Message']}")
            return {"annual": [], "quarterly": []}
        if "Information" in data:
            print(f"[{symbol}] Thông tin từ API: {data['Information']}")
            return {"annual": [], "quarterly": []}
        return data
    except requests.exceptions.Timeout:
        print(f"[{symbol}] Lỗi: Hết thời gian chờ khi gọi API.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Lỗi kết nối đến API: {e}")
        return None
    except Exception as e:
        print(f"[{symbol}] Lỗi không xác định đã xảy ra: {e}")
        return None
    
    
def transform_company_info(raw_data, symbol):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    df['symbol'] = symbol
    df =df.where(pd.notnull(df), None)
    numeric_cols = ['price', 'marketCap', 'beta', 'lastDividend', 'change', 'changePercentage', 'volume', 'averageVolume', 'fullTimeEmployees']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    column_mapping = {
    'price': 'price',
    'marketCap': 'market_cap',
    'beta': 'beta',
    'lastDividend': 'last_dividend',
    'range': 'price_range',
    'change': 'change_value',
    'changePercentage': 'change_percentage',
    'volume': 'volume',
    'averageVolume': 'average_volume',
    'companyName': 'company_name',
    'currency': 'currency',
    'cik': 'cik',
    'isin': 'isin',
    'cusip': 'cusip',
    'exchangeFullName': 'exchange_full_name',
    'exchange': 'exchange_code',
    'industry': 'industry',
    'sector': 'sector',
    'ceo': 'ceo',
    'website': 'website',
    'description': 'description',
    'country': 'country',
    'fullTimeEmployees': 'full_time_employees',
    'phone': 'phone',
    'address': 'address',
    'city': 'city',
    'state': 'state',
    'zip': 'zip',
    'image': 'image',
    'ipoDate': 'ipo_date',
    'defaultImage': 'default_image',
    'isEtf': 'is_etf',
    'isActivelyTrading': 'is_actively_trading',
    'isAdr': 'is_adr',
    'isFund': 'is_fund'
    }
    df = df.rename(columns=column_mapping)
    df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
    ordered_cols = ['symbol','company_name','price','market_cap','beta','last_dividend','price_range',
                    'change_value','change_percentage','volume','average_volume','currency','cik','isin','cusip',
                    'exchange_full_name','exchange_code','industry','sector','ceo','website','description','country',
                    'full_time_employees','phone','address','city','state','zip','image','ipo_date','default_image','is_etf','is_actively_trading','is_adr','is_fund']
    df = df[[col for col in ordered_cols if col in df.columns]]
    return df

def load_company_info_to_db(df, engine, symbol):
    if df.empty:
        print(f"[{symbol}] Không có dữ liệu để ghi.", flush=True)
        return
    with engine.connect() as connection:
        metadata = MetaData()
        table = Table('FMP_company_information', metadata, autoload_with=engine)
        data_to_insert = df.to_dict(orient='records')
        transaction = connection.begin()
        try: 
            connection.execute(table.insert(), data_to_insert)
            transaction.commit()
            
            # Lấy số lượng bản ghi sau khi commit để đảm bảo tính chính xác
            query = text("SELECT COUNT(*) FROM FMP_company_information WHERE symbol = :symbol")
            count = connection.execute(query, {"symbol": symbol}).scalar()
            print(f"[{symbol}] ✅ Đã ghi thành công {len(data_to_insert)} bản ghi. Tổng số bản ghi cho mã này: {count}.", flush=True)
            
        except Exception as e:
            print(f"[{symbol}] ❌ Lỗi khi ghi dữ liệu vào DB: {e}", flush=True)
            transaction.rollback()
            
def fetch_company_information():
    load_dotenv('.env')
    api_key = os.getenv("FINANCIAL_MODELING_PREP_API_KEY")
    if not api_key:
        raise ValueError("API key cho AlphaVantage không được tìm thấy trong file .env")
    
    engine = get_db_engine()
    stock_list_path = os.path.join(os.path.dirname(__file__), "..", "..", "stock_list.csv")
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return
    
    print(f"Bắt đầu fetch dữ liệu lưu chuyển tiền tệ cho các mã: {tickers}")
    
    for symbol in tickers:
        print(f"[{symbol}] Bắt đầu fetch thông tin công ty...", flush=True)
        raw_data = fetch_company_info(symbol, api_key)
        if raw_data is None:
            print(f"[{symbol}] ❌ Lỗi khi fetch dữ liệu. Bỏ qua mã này.", flush=True)
            continue
        transformed_df = transform_company_info(raw_data, symbol)
        load_company_info_to_db(transformed_df, engine, symbol)
        sleep(15)  # Giữ khoảng cách giữa các yêu cầu để tránh bị giới hạn tốc độ

    print("Hoàn tất quá trình fetch thông tin công ty.", flush=True)
    
# CHẠY TỪ DÒNG LỆNH
# ============================================================
if __name__ == "__main__":
    fetch_company_information()