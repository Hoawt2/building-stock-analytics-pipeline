import os
import psycopg2
from dotenv import load_dotenv


load_dotenv('/opt/airflow/.env')
DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT", 5432)


STAGING_TABLE = "staging.stg_alphavantage_earnings"
FACT_TABLE = "dwh.fact_earnings"
DIM_COMPANY_TABLE = "dwh.dim_company_informations"
DIM_TIME_TABLE = "dwh.dim_time"


MEASURE_ATTRIBUTES = [
    "reported_eps", "estimated_eps", "surprise", 
    "surprise_percentage", "reported_time"
]

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

def transform_fact_earnings():
    """Thực hiện Transform tăng dần (Incremental Load) cho Fact Earnings."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        
        all_measure_cols_str = ", ".join(MEASURE_ATTRIBUTES)
        transform_query = f"""
        WITH HWM AS (
            -- Lấy mốc thời gian tải cuối cùng
            SELECT COALESCE(MAX(dw_load_timestamp), '1900-01-01'::TIMESTAMP) AS max_timestamp
            FROM {FACT_TABLE}
        ),
        
        New_Earnings_Data AS (
            SELECT
                -- Surrogate Keys
                dc.company_key,
                -- Khóa cho ngày kết thúc kỳ kế toán (Fiscal Date)
                dt_fiscal.date_key AS fiscal_date_key,
                -- Khóa cho ngày báo cáo (Reported Date)
                dt_reported.date_key AS reported_date_key,
                
                -- Dimensions/Attributes
                UPPER(SUBSTRING(s.report_type, 1, 10)) AS report_type_code,
                
                -- Measures (đã là DECIMAL trong Staging, không cần CAST)
                s.reported_eps,
                s.estimated_eps,
                s.surprise,
                s.surprise_percentage,
                s.reported_time,
                
                s.load_timestamp
            FROM {STAGING_TABLE} s
            
            -- Tra cứu Khóa Công ty (Chỉ JOIN với bản ghi is_current = TRUE)
            JOIN {DIM_COMPANY_TABLE} dc 
                ON s.symbol = dc.symbol AND dc.is_current = TRUE
            
            -- Tra cứu Khóa Ngày kết thúc kỳ (Fiscal Date)
            JOIN {DIM_TIME_TABLE} dt_fiscal
                ON s.fiscal_date_ending = dt_fiscal.full_date
                
            -- Tra cứu Khóa Ngày Báo cáo (Reported Date)
            -- Dùng LEFT JOIN vì reported_date có thể NULL trong Staging, và Reported_Date_Key cũng cho phép NULL
            LEFT JOIN {DIM_TIME_TABLE} dt_reported
                ON s.reported_date = dt_reported.full_date
            WHERE s.load_timestamp > (SELECT max_timestamp FROM HWM)
        )
        
        -- Thực hiện UPSERT: INSERT nếu bản ghi chưa tồn tại, UPDATE nếu đã tồn tại
        INSERT INTO {FACT_TABLE} (
            company_key, fiscal_date_key, reported_date_key, report_type_code, 
            reported_eps, estimated_eps, surprise, surprise_percentage, reported_time, 
            dw_load_timestamp
        )
        SELECT
            company_key, fiscal_date_key, reported_date_key, report_type_code, 
            reported_eps, estimated_eps, surprise, surprise_percentage, reported_time, 
            load_timestamp
        FROM New_Earnings_Data
        
        -- Xử lý xung đột trên UNIQUE CONSTRAINT (company_key, fiscal_date_key, report_type_code)
        ON CONFLICT (company_key, fiscal_date_key, report_type_code) 
        DO UPDATE SET
            -- Cập nhật Reported Date Key (có thể thay đổi nếu Reported Date được thêm/sửa)
            reported_date_key = EXCLUDED.reported_date_key, 
            -- Cập nhật tất cả các cột Measure/Attribute
            {', '.join([f'{col} = EXCLUDED.{col}' for col in MEASURE_ATTRIBUTES])},
            dw_load_timestamp = EXCLUDED.dw_load_timestamp;
        """
        
        cur.execute(transform_query)
        processed_rows = cur.rowcount
        conn.commit()
        
        print(f"Transform Fact Earnings hoàn tất. Đã xử lý {processed_rows} bản ghi (Insert/Update).")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Transform Fact Earnings: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    transform_fact_earnings()