import os
import psycopg2
from dotenv import load_dotenv


load_dotenv('/opt/airflow/.env')

DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT")

STAGING_TABLE = "staging.stg_raw_yfinance"
FACT_TABLE = "dwh.fact_history_stock"
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

def transform_fact_history_stock():
    """Thực hiện Transform tăng dần (Incremental Load) cho Fact History Stock."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        # --- 2. XÁC ĐỊNH HIGH-WATER MARK (HWM) VÀ BẮT ĐẦU TRANSFORM ---
        
        # Truy vấn HWM và chuẩn bị lệnh Transform (sử dụng CTE và UPSERT)
        # HWM được tính toán trực tiếp trong CTE để tạo ra một câu lệnh SQL duy nhất,
        # giúp quá trình thực thi nhanh chóng và nguyên tử (atomic).
        
        transform_query = f"""
        WITH HWM AS (
            -- Lấy mốc thời gian tải cuối cùng hoặc '1900-01-01' nếu bảng trống
            SELECT COALESCE(MAX(dw_load_timestamp), '1900-01-01'::TIMESTAMP) AS max_timestamp
            FROM {FACT_TABLE}
        ),
        
        New_Stock_Data AS (
            SELECT
                -- Surrogate Keys
                dc.company_key,
                dt.date_key,
                -- Measures and Attributes
                s.open_price,
                s.high_price,
                s.low_price,
                s.close_price,
                s.volume,
                s.load_timestamp
            FROM {STAGING_TABLE} s
            -- Lọc dữ liệu tăng dần: Chỉ lấy các bản ghi mới hơn HWM
            -- Tra cứu Khóa Công ty (Chỉ JOIN với bản ghi is_current = TRUE của Dim Company)
            JOIN {DIM_COMPANY_TABLE} dc 
                ON s.symbol = dc.symbol AND dc.is_current = TRUE
            -- Tra cứu Khóa Ngày
            JOIN {DIM_TIME_TABLE} dt 
                ON s.date = dt.full_date
            WHERE s.load_timestamp > (SELECT max_timestamp FROM HWM)
        )
        
        -- Thực hiện UPSERT: INSERT nếu bản ghi chưa tồn tại, UPDATE nếu đã tồn tại
        INSERT INTO {FACT_TABLE} (
            company_key, date_key, open_price, high_price, low_price, 
            close_price, volume, dw_load_timestamp
        )
        SELECT
            company_key, date_key, open_price, high_price, low_price, 
            close_price, volume, load_timestamp
        FROM New_Stock_Data
        -- Xử lý xung đột trên UNIQUE CONSTRAINT (company_key, date_key)
        ON CONFLICT (company_key, date_key) 
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            -- Cập nhật timestamp để ghi nhận bản ghi này đã được xử lý lại
            dw_load_timestamp = EXCLUDED.dw_load_timestamp;
        """
        
        cur.execute(transform_query)
        processed_rows = cur.rowcount
        conn.commit()
        
        print(f"Transform Fact History Stock hoàn tất. Đã xử lý {processed_rows} bản ghi (Insert/Update).")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Transform Fact: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    transform_fact_history_stock()