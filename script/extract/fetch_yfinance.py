import os   
import argparse
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timedelta


# Cấu hình kết nối MINIO 
load_dotenv()
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', "http://localhost:9000")  # Nếu chạy trong docker thì là http://minio:9000
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minioadmin")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minioadmin")
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', "stock-data")
# Đường dẫn ảo S3 tới file Parquet
S3_FILE_PATH = f"s3://{BUCKET_NAME}/raw_parquet/raw_yfinance.parquet"
# Cấu hình "chìa khoá" để Pandas biết đường mở cửa MinIO
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

def get_max_date(symbol):
    """
    Lấy ngày lớn nhất trong file parquet
    """
    try: 
        # Đọc file parquet, chỉ lấy cột symbol và date để tìm ngày lớn nhất
        df = pd.read_parquet(S3_FILE_PATH, columns=['symbol', 'date'], storage_options=STORAGE_OPTIONS)
        df_symbol = df[df['symbol'] == symbol]
        
        if not df_symbol.empty: 
            max_date = df_symbol['date'].max()
            return pd.to_datetime(max_date)
    except Exception as e: 
        print(f"Lỗi khi lấy maxDate trong file Parquet hoặc file không tồn tại trên MINIO: {e}")

    return None 

def fetch_yfinance_data(mode='daily'):
    start_date_historical = os.getenv('START_DATE', '2015-01-01')

    stock_list_path = os.path.join(os.path.dirname(__file__), '..', 'stock_list.csv')
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except Exception as e: 
        print(f"Lỗi khi đọc file stock_list.csv: {e}")
        return
    
    print(f'Bắt đầu fetch dữ liệu cho các mã:{tickers}')
    
    all_new_data = []
    end_date = datetime.now()
    for ticker in tickers:
        start_date = None 
        if mode == 'daily':
            max_date = get_max_date(ticker)
            if max_date:
                start_date = max_date + timedelta(days=1)
                print(f'Ticker {ticker} đã có dữ liệu đến ngày {max_date}, bắt đầu fetch từ ngày {start_date} đến {end_date}')
            else:
                start_date = pd.to_datetime(start_date_historical)
                print(f'Ticker mới {ticker}, bắt đầu fetch từ ngày {start_date} đến {end_date}')
        else:
            start_date = pd.to_datetime(start_date_historical)
            print(f'Ticker {ticker}, Historical load. Fetch từ: {start_date} đến {end_date}')

        if start_date and start_date < end_date: 
            try: 
                # Gọi API yfinance để lấy dữ liệu
                data = yf.download(ticker, start=start_date, end=end_date, progress=False)
                if not data.empty: 
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.get_level_values(0)

                    data.reset_index(inplace=True)
                    data['symbol'] = ticker 
                    data.rename(columns={
                        'Date': 'date',
                        'Open': 'open_price',
                        'High': 'high_price',
                        'Low': 'low_price',
                        'Close': 'close_price',
                        'Volume': 'volume'
                    }, inplace=True)
                    
                    data_to_load = data[['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'symbol']]
                    all_new_data.append(data_to_load)
                    print(f'[{ticker}] Tải thành công {len(data_to_load)} dòng dữ liệu')
                else:
                    print(f'[{ticker}] Không có dữ liệu mới')
            except Exception as e: 
                print(f'[{ticker}] Lỗi khi fetch API: {e}')

    # Ghi toàn bộ dữ liệu vào file parquet 
    if all_new_data: 
        new_df = pd.concat(all_new_data, ignore_index=True)
        
        try:
            print("Đang append dữ liệu vào file Parquet...")
            old_df = pd.read_parquet(S3_FILE_PATH, storage_options=STORAGE_OPTIONS)
            final_df = pd.concat([old_df, new_df], ignore_index=True)

            # Khử trùng lặp 
            final_df.drop_duplicates(subset=['symbol', 'date'], keep='last', inplace=True)
        except Exception: 
            print("Không thấy file cũ trên MinIO. Tạo file Parquet mới tinh!")
            final_df = new_df 
        
        final_df['load_timestamp'] = pd.Timestamp.now()
        final_df.to_parquet(S3_FILE_PATH, index=False, storage_options=STORAGE_OPTIONS)
        print(f"Đã ghi {len(final_df)} dòng dữ liệu vào file Parquet")
    else: 
        print("Không có dữ liệu mới để ghi")

if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Fetch yfinance data')
    parser.add_argument('--mode', type=str, default='daily', choices=['daily', 'historical'], help='Mode to fetch data')
    args = parser.parse_args()
    fetch_yfinance_data(mode=args.mode)                   
        