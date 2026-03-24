WITH source AS ( 
    SELECT * FROM "de_psql"."raw"."raw_earnings" 
),

staged AS ( 
    SELECT 
        md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fiscal_date_ending as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(report_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS earnings_id,
        CAST(TRIM(symbol) AS VARCHAR(50)) as symbol, 
        CAST(TRIM(report_type) AS VARCHAR(50)) as report_type,
        CAST(fiscal_date_ending AS DATE) AS fiscal_date_ending,
        CAST(reported_date AS DATE) AS reported_date,
        CAST(reported_eps AS FLOAT) AS reported_eps,
        CAST(estimated_eps AS FLOAT) AS estimated_eps,
        CAST(surprise AS FLOAT) AS surprise,
        CAST(surprise_percentage AS FLOAT) AS surprise_percentage,
        CAST(report_time AS VARCHAR(10)) AS report_time,
        load_timestamp AS source_load_timestamp,
        CURRENT_TIMESTAMP AS dbt_processed_at

    FROM source
)
SELECT * FROM staged