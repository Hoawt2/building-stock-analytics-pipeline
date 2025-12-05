import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT", 5432)

STAGING_TABLE = "staging.stg_fmp_company_market_cap"
FACT_TABLE = "dwh.fact_market_cap"
DIM_COMPANY_TABLE = "dwh.dim_company_informations"
DIM_TIME_TABLE = "dwh.dim_time"

def get_db_connection():
    """Thiết lập kết nối với PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Lỗi kết nối cơ sở dữ liệu: {e}")
        return None

def transform_fact_market_cap():
    """Thực hiện Transform tăng dần (Incremental Load) cho Fact Marketcap."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        transform_query = f"""
        WITH HWM AS (
            -- Lấy mốc thời gian tải cuối cùng hoặc '1900-01-01' nếu bảng trống
            SELECT COALESCE(MAX(dw_load_timestamp), '1900-01-01'::TIMESTAMP) AS max_timestamp
            FROM {FACT_TABLE}
        ),
        
        New_Cash_Flow_Data AS (
            SELECT
                -- Surrogate Keys
                dc.company_key,
                dt.date_key AS date_key,
                s.market_cap::DECIMAL(30,4) AS market_cap,
                s.load_timestamp
            FROM {STAGING_TABLE} s
            
            -- Tra cứu Khóa Công ty (Chỉ JOIN với bản ghi is_current = TRUE)
            JOIN {DIM_COMPANY_TABLE} dc 
                ON s.symbol = dc.symbol AND dc.is_current = TRUE
            
            -- Tra cứu Khóa Ngày 
            JOIN {DIM_TIME_TABLE} dt 
                ON s.date = dt.full_date
            WHERE s.load_timestamp > (SELECT max_timestamp FROM HWM)
        )
        
        -- Thực hiện UPSERT: INSERT nếu bản ghi chưa tồn tại, UPDATE nếu đã tồn tại
        INSERT INTO {FACT_TABLE} (
            company_key, date_key, 
            market_cap, 
            dw_load_timestamp
        )
        SELECT
            company_key, date_key,  
            market_cap, 
            load_timestamp
        FROM New_Cash_Flow_Data
        
        -- Xử lý xung đột trên UNIQUE CONSTRAINT (company_key, date_key)
        ON CONFLICT (company_key, date_key) 
        DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            dw_load_timestamp = EXCLUDED.dw_load_timestamp;
        """
        
        cur.execute(transform_query)
        processed_rows = cur.rowcount
        conn.commit()
        
        print(f"Transform Fact Marketcap hoàn tất. Đã xử lý {processed_rows} bản ghi (Insert/Update).")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Transform Fact Marketcap: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    transform_fact_market_cap()