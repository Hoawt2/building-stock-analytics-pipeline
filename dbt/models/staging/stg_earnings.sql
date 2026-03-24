WITH source AS ( 
    SELECT * FROM {{ source('de_postgres', 'raw_earnings') }} 
),

staged AS ( 
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['symbol', 'fiscal_date_ending', 'report_type'])}} AS earnings_id,
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