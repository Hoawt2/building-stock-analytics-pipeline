
  create view "de_psql"."staging"."stg_cash_flow__dbt_tmp"
    
    
  as (
    WITH source AS ( 
    SELECT * FROM "de_psql"."raw"."raw_cashflow" 
),

staged AS ( 
    SELECT 
        md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fiscal_date_ending as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(report_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS cash_flow_id,
        CAST(TRIM(symbol) AS VARCHAR(50)) as symbol, 
        CAST(TRIM(report_type) AS VARCHAR(50)) as report_type,
        CAST(fiscal_date_ending AS DATE) AS fiscal_date_ending,
        CAST(TRIM(reported_currency) AS VARCHAR(10)) AS reported_currency,
        CAST(operating_cashflow AS BIGINT) AS operating_cashflow,
	    CAST(payments_for_operating_activities AS BIGINT) AS payments_for_operating_activities,
	    CAST(proceeds_from_operating_activities AS BIGINT) AS proceeds_from_operating_activities,
	    CAST(change_in_operating_liabilities AS BIGINT) AS change_in_operating_liabilities,
	    CAST(change_in_operating_assets AS BIGINT) AS change_in_operating_assets,
	    CAST(depreciation_depletion_and_amortization AS BIGINT) AS depreciation_depletion_and_amortization,
	    CAST(change_in_receivables AS BIGINT) AS change_in_receivables,
	    CAST(change_in_inventory AS BIGINT) AS change_in_inventory,
	    CAST(capital_expenditures AS BIGINT) AS capital_expenditures,
        CAST(cashflow_from_investment AS BIGINT) AS cashflow_from_investment,
        CAST(cashflow_from_financing AS BIGINT) AS cashflow_from_financing,
        CAST(proceeds_from_repayments_of_short_term_debt AS BIGINT) AS proceeds_from_repayments_of_short_term_debt,
        CAST(payments_for_repurchase_of_common_stock AS BIGINT) AS payments_for_repurchase_of_common_stock,
        CAST(payments_for_repurchase_of_equity AS BIGINT) AS payments_for_repurchase_of_equity,
        CAST(payments_for_repurchase_of_preferred_stock AS BIGINT) AS payments_for_repurchase_of_preferred_stock,
        CAST(dividend_payout AS BIGINT) AS dividend_payout,
        CAST(dividend_payout_common_stock AS BIGINT) AS dividend_payout_common_stock,
        CAST(dividend_payout_preferred_stock AS BIGINT) AS dividend_payout_preferred_stock,
        CAST(proceeds_from_issuance_of_common_stock AS BIGINT) AS proceeds_from_issuance_of_common_stock,
        CAST(proceeds_from_issuance_of_preferred_stock AS BIGINT) AS proceeds_from_issuance_of_preferred_stock,
        CAST(proceeds_from_repurchase_of_equity AS BIGINT) AS proceeds_from_repurchase_of_equity,
        CAST(change_in_cash_and_cash_equivalents AS BIGINT) AS change_in_cash_and_cash_equivalents,
        CAST(change_in_exchange_rate AS BIGINT) AS change_in_exchange_rate,
        CAST(net_income AS BIGINT) AS net_income,
        load_timestamp AS source_load_timestamp,
        CURRENT_TIMESTAMP AS dbt_processed_at

    FROM source
)
SELECT * FROM staged
  );