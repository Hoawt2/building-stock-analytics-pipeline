WITH income_statement AS (
    SELECT * FROM "de_psql"."staging"."stg_income_statement"
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
    inc.income_statement_id,
    c.company_key,
    inc.symbol,
    inc.report_type,    

    d.date_key AS fiscal_date_ending_key,
    

    inc.reported_currency,
    inc.total_revenue,
    inc.gross_profit,
    inc.cost_of_revenue,
    inc.cost_of_goods_and_services_sold,
    inc.operating_income,
    inc.selling_general_and_administrative,
    inc.research_and_development,
    inc.operating_expenses,
    inc.investment_income_net,
    inc.net_interest_income,
    inc.interest_income,
    inc.interest_expense,
    inc.non_interest_income,
    inc.other_non_operating_income,
    inc.depreciation,
    inc.depreciation_and_amortization,
    inc.income_before_tax,
    inc.income_tax_expense,
    inc.interest_and_debt_expense,
    inc.net_income_from_continuing_operations,
    inc.comprehensive_income_net_of_tax,
    inc.ebit,
    inc.ebitda,
    inc.net_income,
    
    -- Metadata
    inc.source_load_timestamp,
    inc.dbt_processed_at

FROM income_statement inc
-- Nối Dim Company theo đúng dòng thời gian như Fact Stock Price
LEFT JOIN companies c 
    ON inc.symbol = c.symbol 
    AND inc.fiscal_date_ending >= c.valid_from 
    AND inc.fiscal_date_ending < c.valid_to
    
LEFT JOIN dates d
    ON inc.fiscal_date_ending = d.full_date