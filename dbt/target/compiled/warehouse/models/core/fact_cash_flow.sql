WITH cash_flow AS (
    SELECT * FROM "de_psql"."staging"."stg_cash_flow"
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
    cf.cash_flow_id,
    c.company_key,
    cf.symbol,
    cf.report_type,    

    d.date_key AS fiscal_date_ending_key,
    

    cf.reported_currency,
    cf.operating_cashflow,
    cf.payments_for_operating_activities,
    cf.proceeds_from_operating_activities,
    cf.change_in_operating_liabilities,
    cf.change_in_operating_assets,
    cf.depreciation_depletion_and_amortization,
    cf.change_in_receivables,
    cf.change_in_inventory,
    cf.capital_expenditures,
    cf.cashflow_from_investment,
    cf.cashflow_from_financing,
    cf.proceeds_from_repayments_of_short_term_debt,
    cf.payments_for_repurchase_of_common_stock,
    cf.payments_for_repurchase_of_equity,
    cf.payments_for_repurchase_of_preferred_stock,
    cf.dividend_payout,
    cf.dividend_payout_common_stock,
    cf.dividend_payout_preferred_stock,
    cf.proceeds_from_issuance_of_common_stock,
    cf.proceeds_from_issuance_of_preferred_stock,
    cf.proceeds_from_repurchase_of_equity,
    cf.change_in_cash_and_cash_equivalents,
    cf.change_in_exchange_rate,
    cf.net_income,
    
    -- Metadata
    cf.source_load_timestamp,
    cf.dbt_processed_at

FROM cash_flow cf
-- Nối Dim Company theo đúng dòng thời gian như Fact Stock Price
LEFT JOIN companies c 
    ON cf.symbol = c.symbol 
    AND cf.fiscal_date_ending >= c.valid_from 
    AND cf.fiscal_date_ending < c.valid_to
    
LEFT JOIN dates d
    ON cf.fiscal_date_ending = d.full_date