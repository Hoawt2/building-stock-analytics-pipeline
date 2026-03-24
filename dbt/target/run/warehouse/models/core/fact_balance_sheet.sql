
  
    

  create  table "de_psql"."core"."fact_balance_sheet__dbt_tmp"
  
  
    as
  
  (
    WITH balance_sheet AS (
    SELECT * FROM "de_psql"."staging"."stg_balance_sheet"
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
    bs.balance_sheet_id,
    c.company_key,
    bs.symbol,
    bs.report_type,    

    d.date_key AS fiscal_date_ending_key,
    

    bs.reported_currency,
    bs.total_assets,
    bs.total_current_assets,
    bs.cash_and_cash_equivalents_at_carrying_value,
    bs.cash_and_short_term_investments,
    bs.inventory,
    bs.current_net_receivables,
    bs.total_non_current_assets,
    bs.property_plant_equipment,
    bs.accumulated_depreciation_amortization_ppe,
    bs.intangible_assets,
    bs.intangible_assets_excluding_goodwill,
    bs.goodwill,
    bs.investments,
    bs.long_term_investments,
    bs.short_term_investments,
    bs.other_current_assets,
    bs.other_non_current_assets,
    bs.total_liabilities,
    bs.total_current_liabilities,
    bs.current_accounts_payable,
    bs.deferred_revenue,
    bs.current_debt,
    bs.short_term_debt,
    bs.total_non_current_liabilities,
    bs.capital_lease_obligations,
    bs.long_term_debt,
    bs.current_long_term_debt,
    bs.long_term_debt_noncurrent,
    bs.short_long_term_debt_total,
    bs.other_current_liabilities,
    bs.other_non_current_liabilities,
    bs.total_shareholder_equity,
    bs.treasury_stock,
    bs.retained_earnings,
    bs.common_stock,
    bs.common_stock_shares_outstanding,
    
    -- Metadata
    bs.source_load_timestamp,
    bs.dbt_processed_at

FROM balance_sheet bs
-- Nối Dim Company theo đúng dòng thời gian như Fact Stock Price
LEFT JOIN companies c 
    ON bs.symbol = c.symbol 
    AND bs.fiscal_date_ending >= c.valid_from 
    AND bs.fiscal_date_ending < c.valid_to
    
LEFT JOIN dates d
    ON bs.fiscal_date_ending = d.full_date
  );
  