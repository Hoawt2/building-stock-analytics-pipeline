WITH daily_prices AS (
    SELECT 
        company_key,
        symbol,
        date_key,
        open_price,   -- Nhớ lấy thêm open 
        high_price,   -- Nhớ lấy thêm high
        low_price,    -- Nhớ lấy thêm low
        close_price,
        volume
    FROM "de_psql"."core"."fact_stock_price"
),
companies AS (
    SELECT company_key, company_name, industry
    FROM "de_psql"."core"."dim_company"
    WHERE is_current = TRUE
)

SELECT
    p.company_key,
    p.symbol,
    c.company_name,
    c.industry,
    p.date_key AS trading_date,
    p.close_price,
    p.volume,
    
    -- 1. Daily Return % (Mức sinh lời so với phiên trước)
    ROUND(
        ((p.close_price - LAG(p.close_price) OVER (PARTITION BY p.symbol ORDER BY p.date_key)) 
        / NULLIF(LAG(p.close_price) OVER (PARTITION BY p.symbol ORDER BY p.date_key), 0) * 100)::NUMERIC, 
    2) AS daily_return_pct,

    -- 2. Daily Spread % (Biên độ dao động trong phiên)
    ROUND(
        ((p.high_price - p.low_price) / NULLIF(p.low_price, 0) * 100)::NUMERIC,
    2) AS daily_spread_pct,

    -- 3. Moving Average 7 ngày (MA7)
    ROUND(AVG(p.close_price) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::NUMERIC, 2) AS ma_7_days,
    
    -- 4. Moving Average 30 ngày (MA30)
    ROUND(AVG(p.close_price) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::NUMERIC, 2) AS ma_30_days,

    -- 5. Average Volume 20 ngày (Thanh khoản trung bình 20 phiên)
    ROUND(AVG(p.volume) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )::NUMERIC, 0) AS avg_volume_20_days,

    -- 6. Đỉnh cao nhất 20 phiên (20-Day High)
    MAX(p.high_price) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS high_20_days,

    -- 7. Đáy thấp nhất 20 phiên (20-Day Low)
    MIN(p.low_price) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS low_20_days,

    -- 8. Tính độ lệch chuẩn (Biến động rủi ro Volatility) trong 30 ngày
    ROUND(STDDEV(p.close_price) OVER (
        PARTITION BY p.symbol ORDER BY p.date_key ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::NUMERIC, 2) AS volatility_30_days

FROM daily_prices p
JOIN companies c ON p.company_key = c.company_key