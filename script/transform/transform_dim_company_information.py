import os
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv

load_dotenv(".env")

DB_HOST = "localhost"
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = "5433"

STAGING_TABLE = "staging.stg_fmp_company_information"
DWH_TABLE = "dwh.dim_company_informations"

SCD_TYPE_2_ATTRIBUTES = [
    'company_name',
    'exchange_code',
    'sector',
    'industry',
    'ceo',
    'country',
    'full_time_employees',
    'address',
    'city',
    'state',
    'zip',
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

def transform_dim_company_information():
    """Thực hiện quá trình Transform SCD Type 2 từ Staging sang DWH."""
    conn = get_db_connection()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        
        # Tìm HWM (Max dw_load_timestamp) trong bảng DWH
        cur.execute(f"""
            SELECT COALESCE(MAX(dw_load_timestamp), '1900-01-01'::TIMESTAMP)
            FROM {DWH_TABLE};
        """)
        hwm = cur.fetchone()[0]
        # Ngày hết hiệu lực (End Date) cho các bản ghi cũ sẽ là ngày tải mới nhất trừ đi 1 ngày
        end_date_for_old_record = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Lọc dữ liệu mới (load_timestamp > HWM) và chỉ lấy bản ghi mới nhất cho mỗi symbol
        # Đồng thời LEFT JOIN với bản ghi hiện tại trong DWH để so sánh SCD
        temp_table_query = f"""
        CREATE TEMP TABLE new_company_data AS
        WITH Latest_Staging AS (
            SELECT 
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY s.symbol 
                    ORDER BY s.load_timestamp DESC
                ) AS rn
            FROM {STAGING_TABLE} s
            WHERE s.load_timestamp > '{hwm}'
        )
        SELECT 
            ls.*,
            dc.company_key AS existing_key,
            -- Thuộc tính SCD Type 2 từ DWH hiện tại (để so sánh)
            dc.company_name AS dwh_company_name,
            dc.sector AS dwh_sector,
            dc.industry AS dwh_industry,
            dc.ceo AS dwh_ceo,
            dc.exchange_code AS dwh_exchange_code,
            dc.country AS dwh_country,
            dc.address AS dwh_address
        FROM Latest_Staging ls
        LEFT JOIN {DWH_TABLE} dc 
            ON ls.symbol = dc.symbol 
            AND dc.is_current = TRUE
        WHERE ls.rn = 1;  -- Chỉ giữ lại bản ghi mới nhất từ Staging
        """
        cur.execute(temp_table_query)
        print("Đã tạo bảng tạm với dữ liệu mới và thông tin DWH hiện tại.")

        

        # A. Xử lý CẬP NHẬT (Vô hiệu hóa bản ghi cũ)
        # Tìm các bản ghi trong DWH đã có sự thay đổi thuộc tính
        # Điều kiện phát hiện thay đổi (Nếu bất kỳ thuộc tính SCD Type 2 nào khác nhau)
        change_detection_condition = " OR ".join([
            f"(NT.company_name IS DISTINCT FROM T.company_name)",
            f"(NT.sector IS DISTINCT FROM T.sector)",
            f"(NT.industry IS DISTINCT FROM T.industry)",
            f"(NT.ceo IS DISTINCT FROM T.ceo)",
            f"(NT.exchange_code IS DISTINCT FROM T.exchange_code)",
            f"(NT.country IS DISTINCT FROM T.country)",
            f"(NT.address IS DISTINCT FROM T.address)",
        ])

        update_query = f"""
        UPDATE {DWH_TABLE} AS T
        SET 
            valid_to_date = ('{end_date_for_old_record}'::DATE),
            is_current = FALSE
        FROM new_company_data AS NT
        WHERE T.symbol = NT.symbol 
            AND T.is_current = TRUE 
            AND NT.existing_key IS NOT NULL -- Đảm bảo bản ghi tồn tại trong DWH
            AND ({change_detection_condition}); -- Phát hiện thay đổi
        """
        cur.execute(update_query)
        updated_rows = cur.rowcount
        print(f"Đã vô hiệu hóa {updated_rows} bản ghi cũ do thay đổi thuộc tính (SCD Type 2).")

        # B. Xử lý CHÈN (Bản ghi mới hoặc Bản ghi thay thế)
        # Chèn các bản ghi mới (Công ty mới) HOẶC bản ghi thay thế (do SCD Type 2)
        
        insert_query = f"""
        INSERT INTO {DWH_TABLE} (
            symbol, company_name, cik, isin, cusip, exchange_code, exchange_full_name, 
            sector, industry, country, ceo, full_time_employees, address, city, state, 
            zip, ipo_date, is_etf, is_fund, is_adr, is_actively_trading, 
            valid_from_date, is_current, dw_load_timestamp
        )
        SELECT 
            s.symbol, s.company_name, s.cik, s.isin, s.cusip, s.exchange_code, s.exchange_full_name, 
            s.sector, s.industry, s.country, s.ceo, s.full_time_employees, s.address, s.city, s.state, 
            s.zip, s.ipo_date, s.is_etf, s.is_fund, s.is_adr, s.is_actively_trading, 
            s.load_timestamp::DATE AS valid_from_date, -- Ngày tải là ngày bắt đầu có hiệu lực
            TRUE AS is_current, 
            s.load_timestamp AS dw_load_timestamp
        FROM new_company_data s
        WHERE 
            s.existing_key IS NULL -- TH1: Công ty mới (chưa tồn tại trong DWH)
            OR (s.existing_key IS NOT NULL AND s.load_timestamp > (
                SELECT COALESCE(MAX(dw_load_timestamp), '1900-01-01'::TIMESTAMP) 
                FROM {DWH_TABLE} WHERE symbol = s.symbol
            )) -- TH2: Bản ghi đã được UPDATE ở trên (có thay đổi thuộc tính)
        ;
        """
        cur.execute(insert_query)
        inserted_rows = cur.rowcount
        print(f"Đã chèn {inserted_rows} bản ghi mới/thay thế vào Dim Company.")

        # Dọn dẹp và Commit
        cur.execute("DROP TABLE new_company_data;")
        conn.commit()
        print("Transform Dim Company hoàn tất.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi trong quá trình Transform: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    transform_dim_company_information()