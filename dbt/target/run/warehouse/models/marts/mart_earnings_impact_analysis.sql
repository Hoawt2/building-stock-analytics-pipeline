
  
    

  create  table "de_psql"."marts"."mart_earnings_impact_analysis__dbt_tmp"
  
  
    as
  
  (
    WITH earnings AS (
    SELECT 
        company_key,
        symbol,
        reported_date_key AS report_date,
        reported_eps,
        estimated_eps,
        surprise_percentage
    FROM "de_psql"."core"."fact_earnings"
),
prices AS (
    SELECT symbol, date_key AS date, close_price
    FROM "de_psql"."core"."fact_stock_price"
),
companies AS (
    SELECT company_key, company_name, sector
    FROM "de_psql"."core"."dim_company"
    WHERE is_current = TRUE
)

SELECT
    c.company_name,
    c.sector,
    e.symbol,
    e.report_date,
    e.reported_eps,
    e.estimated_eps,
    e.surprise_percentage,
    
    -- Giá chốt phiên đúng vào ngày ra báo cáo (T0)
    p0.close_price AS price_day_0,
    
    -- Giá sau 3 ngày theo lịch (T+3)
    p3.close_price AS price_day_3,
    
    -- Tính tỷ lệ phần trăm thay đổi giá sau sự kiện (Impact)
    ROUND(
        ((p3.close_price - p0.close_price) / p0.close_price * 100)::NUMERIC, 2
    ) AS price_impact_3_days_pct

FROM earnings e
JOIN companies c ON e.company_key = c.company_key

-- Join khéo léo để bốc cổ phiếu ngày đó (T0)
LEFT JOIN prices p0 
    ON e.symbol = p0.symbol 
    AND e.report_date = p0.date

-- Lấy theo công thức (T0 + 3 ngày) để xem sức khoẻ cổ phiếu 
LEFT JOIN prices p3 
    ON e.symbol = p3.symbol 
    AND CAST(TO_CHAR(TO_DATE(e.report_date::TEXT, 'YYYYMMDD') + INTERVAL '3 days', 'YYYYMMDD') AS INT) = p3.date

-- Tránh bị rác data (Null)
WHERE p0.close_price IS NOT NULL 
  AND p3.close_price IS NOT NULL
  );
  