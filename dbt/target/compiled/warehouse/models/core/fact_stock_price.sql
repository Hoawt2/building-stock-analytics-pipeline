WITH prices AS (
    SELECT * FROM "de_psql"."staging"."stg_stock_price"
), 
companies AS (
    -- Lấy ra company_key (chính là SCD ID) 
    SELECT company_key, symbol, valid_from, valid_to
    FROM "de_psql"."core"."dim_company" 
),
dates AS (
    SELECT * FROM "de_psql"."core"."dim_date"
)

SELECT
    p.stock_price_id,
    c.company_key, -- Dùng đúng cái chìa khóa lịch sử SCD
    p.symbol,
    d.date_key,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume,
    p.source_load_timestamp,
    p.dbt_processed_at
FROM prices p
-- Tuyệt chiêu JOIN ngày tháng: Giá cổ phiếu ngày nào thì map với đúng trạng thái công ty của ngày đó!
LEFT JOIN companies c 
    ON p.symbol = c.symbol 
    AND p.date >= (c.valid_from)::date 
    AND p.date < (c.valid_to)::date
LEFT JOIN dates d
    ON p.date = d.full_date