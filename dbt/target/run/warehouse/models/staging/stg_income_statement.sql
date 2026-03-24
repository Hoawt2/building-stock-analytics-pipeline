
  create view "de_psql"."staging"."stg_income_statement__dbt_tmp"
    
    
  as (
    WITH source AS ( 
    SELECT * FROM "de_psql"."raw"."raw_income_statement" 
),

staged AS ( 
    SELECT 
        md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fiscal_date_ending as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(report_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS income_statement_id,
        CAST(TRIM(symbol) AS VARCHAR(50)) as symbol, 
        CAST(TRIM(report_type) AS VARCHAR(50)) as report_type,
        CAST(fiscal_date_ending AS DATE) AS fiscal_date_ending,
        CAST(TRIM(reported_currency) AS VARCHAR(10)) AS reported_currency,
        CAST(total_revenue AS BIGINT) AS total_revenue,
        CAST(gross_profit AS BIGINT) AS gross_profit,
        CAST(cost_of_revenue AS BIGINT) AS cost_of_revenue,
        CAST(cost_of_goods_and_services_sold AS BIGINT) AS cost_of_goods_and_services_sold,
        CAST(operating_income AS BIGINT) AS operating_income,
        CAST(selling_general_and_administrative AS BIGINT) AS selling_general_and_administrative,
        CAST(research_and_development AS BIGINT) AS research_and_development,
        CAST(operating_expenses AS BIGINT) AS operating_expenses,
        CAST(investment_income_net AS BIGINT) AS investment_income_net,
        CAST(net_interest_income AS BIGINT) AS net_interest_income,
        CAST(interest_income AS BIGINT) AS interest_income,
        CAST(interest_expense AS BIGINT) AS interest_expense,
        CAST(non_interest_income AS BIGINT) AS non_interest_income,
        CAST(other_non_operating_income AS BIGINT) AS other_non_operating_income,
        CAST(depreciation AS BIGINT) AS depreciation,
        CAST(depreciation_and_amortization AS BIGINT) AS depreciation_and_amortization,
        CAST(income_before_tax AS BIGINT) AS income_before_tax,
        CAST(income_tax_expense AS BIGINT) AS income_tax_expense,
        CAST(interest_and_debt_expense AS BIGINT) AS interest_and_debt_expense,
        CAST(net_income_from_continuing_operations AS BIGINT) AS net_income_from_continuing_operations,
        CAST(comprehensive_income_net_of_tax AS BIGINT) AS comprehensive_income_net_of_tax,
        CAST(ebit AS BIGINT) AS ebit,
        CAST(ebitda AS BIGINT) AS ebitda,
        CAST(net_income AS BIGINT) AS net_income,
        load_timestamp AS source_load_timestamp,
        CURRENT_TIMESTAMP AS dbt_processed_at

    FROM source
)
SELECT * FROM staged
  );