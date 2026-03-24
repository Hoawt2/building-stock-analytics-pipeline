WITH date_series AS (
    SELECT generate_series(
        '2000-01-01'::date, 
        '2030-12-31'::date, 
        '1 day'::interval
    )::date AS date_actual
)

SELECT

    CAST(TO_CHAR(date_actual, 'YYYYMMDD') AS INT) AS date_key,
    
    -- 2. Ngày chuẩn
    date_actual AS full_date,
    
    -- 3. Trích xuất các con số (Dùng để tính toán/sort)
    EXTRACT(DAY FROM date_actual)::INT AS day_of_month,
    EXTRACT(MONTH FROM date_actual)::INT AS month_number,
    EXTRACT(QUARTER FROM date_actual)::INT AS quarter_number,
    EXTRACT(YEAR FROM date_actual)::INT AS year_number,
    EXTRACT(ISODOW FROM date_actual)::INT AS day_of_week, -- 1=Thứ2, 7=ChủNhật
    EXTRACT(WEEK FROM date_actual)::INT AS week_of_year,
    
    -- 4. Trích xuất text cho Dashboard (Trim để dọn khoảng trắng thừa mặc định của Postgres)
    TRIM(TO_CHAR(date_actual, 'Day')) AS day_name,
    TRIM(TO_CHAR(date_actual, 'Month')) AS month_name,

    -- 5. Cờ đánh dấu True/False (Ví dụ đánh dấu cuối tuần để trừ bớt ngày giao dịch)
    CASE 
        WHEN EXTRACT(ISODOW FROM date_actual) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend,
    
    -- Cờ đánh dấu chốt tháng 
    CASE 
        WHEN date_actual = ((date_trunc('month', date_actual)) + interval '1 month - 1 day')::date THEN TRUE
        ELSE FALSE
    END AS is_month_end

FROM date_series
