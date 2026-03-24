WITH source AS (
    SELECT * FROM "de_psql"."raw"."raw_yfinance"
), 

staged AS ( 
    SELECT 
        md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(date as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS stock_price_id,
        CAST(TRIM(symbol) AS VARCHAR(50)) as symbol, 
        CAST(open_price AS NUMERIC(18,4)) AS open_price,
        CAST(high_price AS NUMERIC(18,4)) AS high_price,
        CAST(low_price AS NUMERIC(18,4)) AS low_price,
        CAST(close_price AS NUMERIC(18,4)) AS close_price,
        CAST(volume AS BIGINT) AS volume,
        CAST(date AS DATE) AS date,
        load_timestamp AS source_load_timestamp,
        CURRENT_TIMESTAMP AS dbt_processed_at

    FROM source
)
SELECT * FROM staged