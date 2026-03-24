
  create view "de_psql"."staging"."stg_balance_sheet__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "de_psql"."raw"."raw_balance_sheet"
), 

staged AS (
    SELECT 
		md5(cast(coalesce(cast(symbol as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fiscal_date_ending as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(report_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS balance_sheet_id,
		CAST(TRIM(symbol) AS VARCHAR(50)) as symbol, 
		CAST(TRIM(report_type) AS VARCHAR(50)) as report_type,
		CAST(fiscal_date_ending AS DATE) AS fiscal_date_ending,
		CAST(TRIM(reported_currency) AS VARCHAR(10)) AS reported_currency,
		CAST(total_assets AS BIGINT) AS total_assets,
		CAST(total_current_assets AS BIGINT) AS total_current_assets,
		CAST(cash_and_cash_equivalents_at_carrying_value AS BIGINT) AS cash_and_cash_equivalents_at_carrying_value,
		CAST(cash_and_short_term_investments AS BIGINT) AS cash_and_short_term_investments,
		CAST(inventory AS BIGINT) AS inventory,
		CAST(current_net_receivables AS BIGINT) AS current_net_receivables,
		CAST(total_non_current_assets AS BIGINT) AS total_non_current_assets,
		CAST(property_plant_equipment AS BIGINT) AS property_plant_equipment,
		CAST(accumulated_depreciation_amortization_ppe AS BIGINT) AS accumulated_depreciation_amortization_ppe,
		CAST(intangible_assets AS BIGINT) AS intangible_assets,
		CAST(intangible_assets_excluding_goodwill AS BIGINT) AS intangible_assets_excluding_goodwill,
		CAST(goodwill AS BIGINT) AS goodwill,
		CAST(investments AS BIGINT) AS investments,
		CAST(long_term_investments AS BIGINT) AS long_term_investments,
		CAST(short_term_investments AS BIGINT) AS short_term_investments,
		CAST(other_current_assets AS BIGINT) AS other_current_assets,
		CAST(other_non_current_assets AS BIGINT) AS other_non_current_assets,
		CAST(total_liabilities AS BIGINT) AS total_liabilities,
		CAST(total_current_liabilities AS BIGINT) AS total_current_liabilities,
		CAST(current_accounts_payable AS BIGINT) AS current_accounts_payable,
		CAST(deferred_revenue AS BIGINT) AS deferred_revenue,
		CAST(current_debt AS BIGINT) AS current_debt,
		CAST(short_term_debt AS BIGINT) AS short_term_debt,
		CAST(total_non_current_liabilities AS BIGINT) AS total_non_current_liabilities,
		CAST(capital_lease_obligations AS BIGINT) AS capital_lease_obligations,
		CAST(long_term_debt AS BIGINT) AS long_term_debt,
		CAST(current_long_term_debt AS BIGINT) AS current_long_term_debt,
		CAST(long_term_debt_noncurrent AS BIGINT) AS long_term_debt_noncurrent,
		CAST(short_long_term_debt_total AS BIGINT) AS short_long_term_debt_total,
		CAST(other_current_liabilities AS BIGINT) AS other_current_liabilities,
		CAST(other_non_current_liabilities AS BIGINT) AS other_non_current_liabilities,
		CAST(total_shareholder_equity AS BIGINT) AS total_shareholder_equity,
		CAST(treasury_stock AS BIGINT) AS treasury_stock,
		CAST(retained_earnings AS BIGINT) AS retained_earnings,
		CAST(common_stock AS BIGINT) AS common_stock,
		CAST(common_stock_shares_outstanding AS BIGINT) AS common_stock_shares_outstanding,
		load_timestamp AS source_load_timestamp,
		CURRENT_TIMESTAMP AS dbt_processed_at

	FROM source
)
SELECT * FROM staged
  );