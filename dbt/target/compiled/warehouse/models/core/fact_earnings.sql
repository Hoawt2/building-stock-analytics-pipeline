WITH earnings AS (
    SELECT * FROM "de_psql"."staging"."stg_earnings"
),
companies AS (
    SELECT company_key, symbol, valid_from, valid_to
    FROM "de_psql"."core"."dim_company"
),
dates AS (
    SELECT date_key, full_date 
    FROM "de_psql"."core"."dim_date"
)

SELECT
    e.earnings_id,
    c.company_key,
    e.symbol,

    e.report_type,    
    -- Cả 2 đều trở thành Khóa Ngoại đóng 2 vai trò khác nhau
    d1.date_key AS reported_date_key, 
    d2.date_key AS fiscal_date_ending_key,
    

    e.reported_eps,
    e.estimated_eps,
    e.surprise,
    e.surprise_percentage,
    e.report_time,
    
    -- Metadata
    e.source_load_timestamp,
    e.dbt_processed_at

FROM earnings e
-- Nối Dim Company theo đúng dòng thời gian như Fact Stock Price
LEFT JOIN companies c 
    ON e.symbol = c.symbol 
    AND e.reported_date >= c.valid_from 
    AND e.reported_date < c.valid_to
    
-- JOIN lần 1: Mời diễn viên đóng vai "Ngày công bố"
LEFT JOIN dates d1
    ON e.reported_date = d1.full_date
-- JOIN lần 2: Mời diễn viên đóng vai "Ngày chốt sổ"
LEFT JOIN dates d2
    ON e.fiscal_date_ending = d2.full_date