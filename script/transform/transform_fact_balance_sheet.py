import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT", 5432)

STAGING_TABLE = "staging.stg_alphavantage_balance_sheet"
FACT_TABLE = "dwh.fact_balance_sheet"
DIM_COMPANY_TABLE = "dwh.dim_company_informations"
DIM_TIME_TABLE = "dwh.dim_time"

MEASURE_COLUMNS = ["total_assets",
"total_current_assets",
"cash_and_cash_equivalents_at_carrying_value",
"cash_and_short_term_investments",
"inventory",
"current_net_receivables",
"total_non_current_assets",
"property_plant_equipment",
"accumulated_depreciation_amortization_ppe",
"intangible_assets",
"intangible_assets_excluding_goodwill",
"goodwill",
"investments",
"long_term_investments",
"short_term_investments",
"other_current_assets",
"other_non_current_assets",

"total_liabilities",
"total_current_liabilities",
"current_accounts_payable",
"deferred_revenue",
"current_debt",
"short_term_debt",
"total_non_current_liabilities",
"capital_lease_obligations",
"long_term_debt",
"current_long_term_debt",
"long_term_debt_noncurrent",
"short_long_term_debt_total",
"other_current_liabilities",
"other_non_current_liabilities",

"total_shareholder_equity",
"treasury_stock",
"retained_earnings",
"common_stock",
"common_stock_shares_outstanding"]

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

def transform_fact_balance_sheet():
    """Thực hiện Transform tăng dần (Incremental Load) cho FACT Balancesheet."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        
        # Tạo danh sách các cột Measure cho câu lệnh SQL, bao gồm CAST
        measure_select_list = ",\n".join([
            f"s.{col}::DECIMAL(30, 4) AS {col}" for col in MEASURE_COLUMNS
        ])
        
        measure_cols_str = ", ".join(MEASURE_COLUMNS)
    
        
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
                dt.date_key AS fiscal_date_key,
                
                -- Dimensions/Attributes
                s.reported_currency,
                -- Ánh xạ 'annual'/'quarterly' thành 'ANNUAL'/'QUARTER' cho report_type_code
                UPPER(SUBSTRING(s.report_type, 1, 10)) AS report_type_code,
                
                -- Measures (BIGINT -> DECIMAL(20, 4))
                {measure_select_list},
                
                s.load_timestamp
            FROM {STAGING_TABLE} s
            
            -- Tra cứu Khóa Công ty (Chỉ JOIN với bản ghi is_current = TRUE)
            JOIN {DIM_COMPANY_TABLE} dc 
                ON s.symbol = dc.symbol AND dc.is_current = TRUE
            
            -- Tra cứu Khóa Ngày (dùng fiscal_date_ending)
            JOIN {DIM_TIME_TABLE} dt 
                ON s.fiscal_date_ending = dt.full_date
            WHERE s.load_timestamp > (SELECT max_timestamp FROM HWM)
        )
        
        -- Thực hiện UPSERT: INSERT nếu bản ghi chưa tồn tại, UPDATE nếu đã tồn tại
        INSERT INTO {FACT_TABLE} (
            company_key, fiscal_date_key, reported_currency, report_type_code, 
            {measure_cols_str}, 
            dw_load_timestamp
        )
        SELECT
            company_key, fiscal_date_key, reported_currency, report_type_code, 
            {measure_cols_str}, 
            load_timestamp
        FROM New_Cash_Flow_Data
        
        -- Xử lý xung đột trên UNIQUE CONSTRAINT (company_key, fiscal_date_key, report_type_code)
        ON CONFLICT (company_key, fiscal_date_key, report_type_code) 
        DO UPDATE SET
            reported_currency = EXCLUDED.reported_currency,
            -- Cập nhật tất cả các cột Measure
            {', '.join([f'{col} = EXCLUDED.{col}' for col in MEASURE_COLUMNS])},
            dw_load_timestamp = EXCLUDED.dw_load_timestamp;
        """
        
        cur.execute(transform_query)
        processed_rows = cur.rowcount
        conn.commit()
        
        print(f"Transform Fact Balancesheet hoàn tất. Đã xử lý {processed_rows} bản ghi (Insert/Update).")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Transform balancesheet: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    transform_fact_balance_sheet()