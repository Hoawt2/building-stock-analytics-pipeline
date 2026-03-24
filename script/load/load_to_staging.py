import os 
import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema 
from sqlalchemy import text
from dotenv import load_dotenv 

# SETUP KẾT NỐI TỚI MINIO 
load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minioadmin")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minioadmin")
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', "stock-data")

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT
    }
}

# SETUP KẾT NỐI TỚI POSTGRESQL 
def get_pg_engine(): 
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')
    user = os.getenv('POSTGRES_USER', 'admin')
    password = os.getenv('POSTGRES_PASSWORD', 'admin')
    db = os.getenv('POSTGRES_DB', 'de_psql')
    
    conn_str = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_str)
    
    with engine.connect() as conn: 
        if not conn.dialect.has_schema(conn, 'raw'):
            conn.execute(CreateSchema('raw'))
            conn.commit()
            print("Đã tạo schema 'raw'.")
        else:
            print("Schema 'raw' đã tồn tại.")

    return engine
# HÀM CDC HIGH_WATERMARK THEO DÕI MỐC THỜI GIAN 
def get_max_load_timestamp(engine, table_name):
    try: 
        with engine.connect() as conn: 
            # Kiểm tra xem bảng đã tồn tại chưa để tránh lỗi UndefinedTable
            check_table = text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = '{table_name}')")
            if not conn.execute(check_table).scalar():
                return None
            
            query = text(f"""
            SELECT MAX(load_timestamp) 
            FROM raw.{table_name}
            """)
            result = conn.execute(query).scalar()
            return result if result else None
    except Exception as e:
        print(f"Lỗi khi lấy max load timestamp: {e}")
        return None
    

# ĐỌC FILE PARQUET VÀ LOAD VÀO RAW AREA 
def load_parquet_to_postgres():
    engine = get_pg_engine()
    
    table_mapping = {
        "raw_yfinance.parquet": "raw_yfinance",
        "alphavantage_balance_sheet.parquet": "raw_balance_sheet",
        "alphavantage_cash_flow.parquet": "raw_cashflow",
        "alphavantage_earnings.parquet": "raw_earnings",
        "alphavantage_income_statement.parquet": "raw_income_statement",
        "fmp_company_information.parquet": "raw_company_information",
    }
    
    print("BẮT ĐẦU QUÁ TRÌNH LOAD TỪ MINIO VÀO RAW")

    for parquet_file, stg_table in table_mapping.items():
        s3_path = f"s3://{BUCKET_NAME}/raw_parquet/{parquet_file}"
        print(f"[{stg_table}] Đang load dữ liệu từ {s3_path}")

        try:
            df = pd.read_parquet(s3_path, storage_options=STORAGE_OPTIONS)
            if df.empty: 
                print(f"[{stg_table}] File rỗng, bỏ qua")
                continue
            df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

            max_ts = get_max_load_timestamp(engine, stg_table)
            if max_ts: 
                df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
                new_df = df[df['load_timestamp'] > max_ts]
                if new_df.empty: 
                    print(f"[{stg_table}] Dữ liệu up-to-date, bỏ qua")
                    continue
                else: 
                    print(f"[{stg_table}] Có {len(new_df)} dòng dữ liệu mới")           
                    new_df.to_sql(name=stg_table, schema='raw', con=engine, if_exists='append', index=False, method='multi', chunksize=5000)
                    print(f"[{stg_table}] Load thành công {len(new_df)} dòng dữ liệu vào PostgreSQL")
            else:
                # Chưa có bảng này (hoặc trả về None) -> Khởi tạo bảng và Load Full lần đầu
                print(f"[{stg_table}] Chưa có data cũ (Initial Load). Lưu toàn bộ {len(df)} dòng...")
                df.to_sql(name=stg_table, schema='raw', con=engine, if_exists='replace', index=False, method='multi', chunksize=5000)

        except Exception as e:
            print(f"[{stg_table}] Lỗi khi load dữ liệu: {e}")

if __name__ == "__main__":
    load_parquet_to_postgres()