
  create view "de_psql"."staging"."stg_company_information__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "de_psql"."raw"."raw_company_information"
), 

staged AS (
    SELECT 
        -- 1. Tạo Surrogate Key (ID duy nhất) từ mã chứng khoán (symbol)
        -- Lưu ý: dbt_utils bản mới dùng 'generate_surrogate_key' thay vì 'surrogate_key'
        md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS company_info_id,
        
        -- 2. Định danh công ty
        CAST(TRIM(symbol) AS VARCHAR(50)) AS symbol,
        CAST(TRIM(COALESCE(company_name, 'Unknown')) AS VARCHAR(255)) AS company_name,
        CAST(TRIM(cik) AS VARCHAR(20)) AS cik_code,
        CAST(TRIM(isin) AS VARCHAR(20)) AS isin_code,
        CAST(TRIM(cusip) AS VARCHAR(20)) AS cusip_code,
        
        -- 3. Phân loại ngành nghề (Sử dụng COALESCE chặn NULL sinh báo cáo lỗi ở Dashboard)
        CAST(TRIM(COALESCE(exchange_full_name, 'Unknown')) AS VARCHAR(255)) AS exchange_full_name,
        CAST(TRIM(COALESCE(exchange_code, 'Unknown')) AS VARCHAR(50)) AS exchange_code,
        CAST(TRIM(COALESCE(industry, 'Unknown')) AS VARCHAR(100)) AS industry,
        CAST(TRIM(COALESCE(sector, 'Unknown')) AS VARCHAR(100)) AS sector,
        
        -- 4. Dữ liệu tài chính (Lưu số là NULL nếu không có dữ liệu để tính Average/Sum chuẩn xác)
        CAST(price AS NUMERIC(18,4)) AS current_price,
        CAST(market_cap AS NUMERIC(24,2)) AS market_cap,
        CAST(beta AS NUMERIC(10,4)) AS beta,
        CAST(last_dividend AS NUMERIC(18,4)) AS last_dividend,
        CAST(change_value AS NUMERIC(18,4)) AS change_value,
        CAST(change_percentage AS NUMERIC(10,4)) AS change_percentage,
        CAST(volume AS BIGINT) AS volume,
        CAST(average_volume AS BIGINT) AS average_volume,
        CAST(TRIM(price_range) AS VARCHAR(100)) AS price_range,
        CAST(TRIM(currency) AS VARCHAR(10)) AS currency,
        
        -- 5. Thông tin chi tiết (Giữ nguyên text và Trim khoảng trắng)
        TRIM(COALESCE(ceo, 'Unknown')) AS ceo,
        TRIM(website) AS website,
        TRIM(description) AS description,
        TRIM(COALESCE(country, 'Unknown')) AS country,
        TRIM(COALESCE(state, 'Unknown')) AS state,
        TRIM(COALESCE(city, 'Unknown')) AS city,
        TRIM(address) AS address,
        TRIM(zip) AS zip_code,
        TRIM(phone) AS phone,
        CAST(full_time_employees AS BIGINT) AS full_time_employees,
        
        -- 6. Boolean Flags
        is_etf,
        is_actively_trading,
        is_adr,
        is_fund,
        
        -- 7. Ngày tháng và metadata (Postgres dùng CAST AS DATE thay vì TRY_TO_DATE của Snowflake)
        CAST(ipo_date AS DATE) AS ipo_date,
        image AS image_url,
        default_image,
        
        -- Metadata lưu vết thời gian
        load_timestamp AS source_load_timestamp,
        CURRENT_TIMESTAMP AS dbt_processed_at

    FROM source
)

SELECT * FROM staged
  );