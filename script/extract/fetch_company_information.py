import os
import requests
import pandas as pd
from dotenv import load_dotenv
from time import sleep


# Cấu hình kết nối MINIO 
load_dotenv()
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', "http://localhost:9000")  # Nếu chạy trong docker thì là http://minio:9000
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minioadmin")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minioadmin")
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', "stock-data")
# Đường dẫn ảo S3 tới file Parquet
S3_FILE_PATH = f"s3://{BUCKET_NAME}/raw_parquet/fmp_company_information.parquet"
# Cấu hình "chìa khoá" để Pandas biết đường mở cửa MinIO
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

# GỌI API FMP
def fetch_company_info(symbol, api_key):
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={api_key}"
    try: 
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        try: 
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"[{symbol}] Lỗi: Không thể giải mã JSON.")
            return None
            
        if "Error Message" in data:
             return None
        if "Information" in data:
             return None
        return data
    except Exception as e:
        print(f"[{symbol}] Lỗi khi gọi API: {e}")
        return None
    
# CHUYỂN ĐỔI DỮ LIỆU
def transform_company_info(raw_data, symbol):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    df['symbol'] = symbol
    df = df.where(pd.notnull(df), None)
    
    numeric_cols = ['price', 'marketCap', 'beta', 'lastDividend', 'change', 'changePercentage', 'volume', 'averageVolume', 'fullTimeEmployees']
    # Chỉ xử lý các cột có tồn tại trong df
    numeric_cols = [c for c in numeric_cols if c in df.columns]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    column_mapping = {
        'price': 'price', 'marketCap': 'market_cap', 'beta': 'beta',
        'lastDividend': 'last_dividend', 'range': 'price_range', 
        'change': 'change_value', 'changePercentage': 'change_percentage',
        'volume': 'volume', 'averageVolume': 'average_volume', 
        'companyName': 'company_name', 'currency': 'currency', 
        'cik': 'cik', 'isin': 'isin', 'cusip': 'cusip',
        'exchangeFullName': 'exchange_full_name', 'exchange': 'exchange_code',
        'industry': 'industry', 'sector': 'sector', 'ceo': 'ceo', 
        'website': 'website', 'description': 'description', 'country': 'country',
        'fullTimeEmployees': 'full_time_employees', 'phone': 'phone', 
        'address': 'address', 'city': 'city', 'state': 'state', 'zip': 'zip',
        'image': 'image', 'ipoDate': 'ipo_date', 'defaultImage': 'default_image',
        'isEtf': 'is_etf', 'isActivelyTrading': 'is_actively_trading',
        'isAdr': 'is_adr', 'isFund': 'is_fund'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    if 'ipo_date' in df.columns:
        df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
        
    ordered_cols = ['symbol', 'company_name', 'price', 'market_cap', 'beta', 
                    'last_dividend', 'price_range', 'change_value', 'change_percentage', 
                    'volume', 'average_volume', 'currency', 'cik', 'isin', 'cusip',
                    'exchange_full_name', 'exchange_code', 'industry', 'sector', 
                    'ceo', 'website', 'description', 'country', 'full_time_employees', 
                    'phone', 'address', 'city', 'state', 'zip', 'image', 'ipo_date', 
                    'default_image', 'is_etf', 'is_actively_trading', 'is_adr', 'is_fund']
                    
    df = df[[col for col in ordered_cols if col in df.columns]]
    return df

# HÀM MAIN VÀ LƯU PARQUET
def fetch_company_information():
    api_key = os.getenv("FINANCIAL_MODELING_PREP_API_KEY")
    if not api_key:
        print("Cảnh báo: Không tìm thấy FINANCIAL_MODELING_PREP_API_KEY trong file .env")
        return
    stock_list_path = os.path.join(os.path.dirname(__file__), '..', 'stock_list.csv')
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {stock_list_path}")
        return
    
    print(f"Bắt đầu fetch dữ liệu Company Info cho các mã: {tickers}")
    
    all_new_data = []
    
    for symbol in tickers:
        print(f"[{symbol}] Bắt đầu fetch thông tin công ty...", flush=True)
        raw_data = fetch_company_info(symbol, api_key)
        
        if not raw_data:
            print(f"[{symbol}] ❌ Không có dữ liệu. Bỏ qua.", flush=True)
        else:
            transformed_df = transform_company_info(raw_data, symbol)
            if not transformed_df.empty:
                all_new_data.append(transformed_df)
                print(f"[{symbol}] ✅ Xử lý thành công {len(transformed_df)} dòng.")
                
        sleep(1) # Tránh spam API dồn dập

    if all_new_data:
        new_df = pd.concat(all_new_data, ignore_index=True)
        new_df['load_timestamp'] = pd.Timestamp.now()
        # MySQL cũ dùng on_duplicate_key_update (nghĩa là ghi đè nếu trùng symbol)
        # Parquet thì ta đọc lên, gộp lại, rồi drop_duplicates trên symbol và giữ bản ghi mới nhất (keep='last')
        try:
            print("Đang gộp dữ liệu cũ vào Parquet và Upsert (Ghi đè bản ghi mới)...")
            old_df = pd.read_parquet(S3_FILE_PATH, storage_options=STORAGE_OPTIONS)
            final_df = pd.concat([old_df, new_df], ignore_index=True)
            final_df.drop_duplicates(subset=['symbol'], keep='last', inplace=True)
        except Exception: 
            print("Không thấy file cũ trên MinIO. Tạo file Parquet mới tinh!")
            final_df = new_df
            
        final_df.to_parquet(S3_FILE_PATH, index=False, storage_options=STORAGE_OPTIONS)
        print(f"✅ Hoàn tất! Đã lưu {len(final_df)} công ty vào Data Lake (Parquet).")
    else:
        print("Không có thông tin công ty nào được cập nhật.")

if __name__ == "__main__":
    fetch_company_information()
