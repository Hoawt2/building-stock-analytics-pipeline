DROP DATABASE IF EXISTS extract_db;
CREATE DATABASE extract_db;
USE extract_db;

DROP TABLE IF EXISTS raw_yfinance;
DROP TABLE IF EXISTS alphavantage_cash_flow;
DROP TABLE IF EXISTS alphavantage_balance_sheet;
DROP TABLE IF EXISTS alphavantage_income_statement;
DROP TABLE IF EXISTS alphavantage_earnings;
DROP TABLE IF EXISTS FMP_company_information;   
DROP TABLE IF EXISTS FMP_company_key_metrics;
DROP TABLE IF EXISTS FMP_company_market_cap;

CREATE TABLE IF NOT EXISTS raw_yfinance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20),
    date DATE,
    open_price DECIMAL(15,2) NOT NULL,
    high_price DECIMAL(15,2),
    low_price DECIMAL(15,2),
    close_price DECIMAL(15,2) NOT NULL,
    volume BIGINT NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    UNIQUE KEY idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS alphavantage_cash_flow (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,                           
    fiscal_date_ending DATE NOT NULL,                     
    reported_currency VARCHAR(10),   
    report_type ENUM('annual', 'quarterly') NOT NULL,                      
    -- Hoạt động kinh doanh
    operating_cashflow BIGINT,
    payments_for_operating_activities BIGINT,
    proceeds_from_operating_activities BIGINT,
    change_in_operating_liabilities BIGINT,
    change_in_operating_assets BIGINT,
    depreciation_depletion_and_amortization BIGINT,
    change_in_receivables BIGINT,
    change_in_inventory BIGINT,

    -- Hoạt động đầu tư
    capital_expenditures BIGINT,
    cashflow_from_investment BIGINT,

    -- Hoạt động tài chính
    cashflow_from_financing BIGINT,
    proceeds_from_repayments_of_short_term_debt BIGINT,
    payments_for_repurchase_of_common_stock BIGINT,
    payments_for_repurchase_of_equity BIGINT,
    payments_for_repurchase_of_preferred_stock BIGINT,
    dividend_payout BIGINT,
    dividend_payout_common_stock BIGINT,
    dividend_payout_preferred_stock BIGINT,
    proceeds_from_issuance_of_common_stock BIGINT,
    proceeds_from_issuance_of_long_term BIGINT,
    proceeds_from_issuance_of_preferred_stock BIGINT,
    proceeds_from_repurchase_of_equity BIGINT,
    proceeds_from_sale_of_treasury_stock BIGINT,

    -- Thông tin tổng hợp
    change_in_cash_and_cash_equivalents BIGINT,
    change_in_exchange_rate BIGINT,
    net_income BIGINT,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_report (symbol, fiscal_date_ending, report_type)
);

CREATE TABLE IF NOT EXISTS alphavantage_balance_sheet (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,                           
    fiscal_date_ending DATE NOT NULL,                      
    reported_currency VARCHAR(10),                         
    report_type ENUM('annual', 'quarterly') NOT NULL,
    -- Tài sản
    total_assets BIGINT,
    total_current_assets BIGINT,
    cash_and_cash_equivalents_at_carrying_value BIGINT,
    cash_and_short_term_investments BIGINT,
    inventory BIGINT,
    current_net_receivables BIGINT,
    total_non_current_assets BIGINT,
    property_plant_equipment BIGINT,
    accumulated_depreciation_amortization_ppe BIGINT,
    intangible_assets BIGINT,
    intangible_assets_excluding_goodwill BIGINT,
    goodwill BIGINT,
    investments BIGINT,
    long_term_investments BIGINT,
    short_term_investments BIGINT,
    other_current_assets BIGINT,
    other_non_current_assets BIGINT,

    -- Nợ phải trả
    total_liabilities BIGINT,
    total_current_liabilities BIGINT,
    current_accounts_payable BIGINT,
    deferred_revenue BIGINT,
    current_debt BIGINT,
    short_term_debt BIGINT,
    total_non_current_liabilities BIGINT,
    capital_lease_obligations BIGINT,
    long_term_debt BIGINT,
    current_long_term_debt BIGINT,
    long_term_debt_noncurrent BIGINT,
    short_long_term_debt_total BIGINT,
    other_current_liabilities BIGINT,
    other_non_current_liabilities BIGINT,

    -- Vốn chủ sở hữu
    total_shareholder_equity BIGINT,
    treasury_stock BIGINT,
    retained_earnings BIGINT,
    common_stock BIGINT,
    common_stock_shares_outstanding BIGINT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_report (symbol, fiscal_date_ending, report_type)
);

CREATE TABLE IF NOT EXISTS alphavantage_income_statement (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,                           
    fiscal_date_ending DATE NOT NULL,                      
    reported_currency VARCHAR(10),                         
    report_type ENUM('annual', 'quarterly') NOT NULL,
    -- Doanh thu và lợi nhuận gộp
    total_revenue BIGINT,
    gross_profit BIGINT,
    cost_of_revenue BIGINT,
    cost_of_goods_and_services_sold BIGINT,

    -- Chi phí hoạt động
    operating_income BIGINT,
    selling_general_and_administrative BIGINT,
    research_and_development BIGINT,
    operating_expenses BIGINT,

    -- Thu nhập và chi phí khác
    investment_income_net BIGINT,
    net_interest_income BIGINT,
    interest_income BIGINT,
    interest_expense BIGINT,
    non_interest_income BIGINT,
    other_non_operating_income BIGINT,
    depreciation BIGINT,
    depreciation_and_amortization BIGINT,

    -- Thu nhập trước và sau thuế
    income_before_tax BIGINT,
    income_tax_expense BIGINT,
    interest_and_debt_expense BIGINT,
    net_income_from_continuing_operations BIGINT,
    comprehensive_income_net_of_tax BIGINT,

    -- Các chỉ tiêu hiệu quả
    ebit BIGINT,
    ebitda BIGINT,
    net_income BIGINT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_report (symbol, fiscal_date_ending, report_type)
);

CREATE TABLE IF NOT EXISTS alphavantage_earnings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    fiscal_date_ending DATE NOT NULL,
    report_type ENUM('annual', 'quarterly') NOT NULL,
    reported_date DATE,
    reported_eps DECIMAL(20,6),
    estimated_eps DECIMAL(20,6),
    surprise DECIMAL(20,6),
    surprise_percentage DECIMAL(10,6),
    reported_time VARCHAR(50),              
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_earnings (symbol, fiscal_date_ending, report_type)
);

CREATE TABLE IF NOT EXISTS FMP_company_information (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    price DECIMAL(15,4),
    market_cap BIGINT,
    beta DECIMAL(10,4),
    last_dividend DECIMAL(10,4),
    price_range VARCHAR(50),
    change_value DECIMAL(10,4),
    change_percentage DECIMAL(10,5),
    volume BIGINT,
    average_volume BIGINT,
    currency VARCHAR(10),
    cik VARCHAR(20),
    isin VARCHAR(20),
    cusip VARCHAR(20),
    exchange_full_name VARCHAR(100),
    exchange_code VARCHAR(50),
    industry VARCHAR(100),
    sector VARCHAR(100),
    ceo VARCHAR(100),
    website VARCHAR(255),
    description TEXT,
    country VARCHAR(50),
    full_time_employees INT,
    phone VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20),
    image VARCHAR(255),
    ipo_date DATE,
    default_image BOOLEAN,
    is_etf BOOLEAN,
    is_actively_trading BOOLEAN,
    is_adr BOOLEAN,
    is_fund BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS FMP_company_key_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    calendar_year YEAR,
    period ENUM('FY', 'Q1', 'Q2', 'Q3', 'Q4') DEFAULT 'FY',

    -- Per Share Metrics
    revenue_per_share DECIMAL(18,6),
    net_income_per_share DECIMAL(18,6),
    operating_cash_flow_per_share DECIMAL(18,6),
    free_cash_flow_per_share DECIMAL(18,6),
    cash_per_share DECIMAL(18,6),
    book_value_per_share DECIMAL(18,6),
    tangible_book_value_per_share DECIMAL(18,6),
    shareholders_equity_per_share DECIMAL(18,6),
    interest_debt_per_share DECIMAL(18,6),
    capex_per_share DECIMAL(18,6),

    -- Valuation Ratios
    market_cap BIGINT,
    enterprise_value BIGINT,
    pe_ratio DECIMAL(18,6),
    price_to_sales_ratio DECIMAL(18,6),
    pocf_ratio DECIMAL(18,6),
    pfcf_ratio DECIMAL(18,6),
    pb_ratio DECIMAL(18,6),
    ptb_ratio DECIMAL(18,6),
    ev_to_sales DECIMAL(18,6),
    enterprise_value_over_ebitda DECIMAL(18,6),
    ev_to_operating_cash_flow DECIMAL(18,6),
    ev_to_free_cash_flow DECIMAL(18,6),

    -- Profitability & Yield
    earnings_yield DECIMAL(18,6),
    free_cash_flow_yield DECIMAL(18,6),
    roe DECIMAL(18,6),
    roic DECIMAL(18,6),
    return_on_tangible_assets DECIMAL(18,6),

    -- Liquidity & Leverage
    debt_to_equity DECIMAL(18,6),
    debt_to_assets DECIMAL(18,6),
    net_debt_to_ebitda DECIMAL(18,6),
    current_ratio DECIMAL(18,6),
    interest_coverage DECIMAL(18,6),

    -- Quality & Efficiency
    income_quality DECIMAL(18,6),
    payout_ratio DECIMAL(18,6),
    dividend_yield DECIMAL(18,6),
    sales_general_and_admin_to_revenue DECIMAL(18,6),
    research_and_development_to_revenue DECIMAL(18,6),
    stock_based_compensation_to_revenue DECIMAL(18,6),

    -- Asset & Capex ratios
    intangibles_to_total_assets DECIMAL(18,6),
    capex_to_operating_cash_flow DECIMAL(18,6),
    capex_to_revenue DECIMAL(18,6),
    capex_to_depreciation DECIMAL(18,6),

    -- Working Capital
    working_capital BIGINT,
    tangible_asset_value BIGINT,
    net_current_asset_value BIGINT,
    invested_capital BIGINT,
    average_receivables BIGINT,
    average_payables BIGINT,
    average_inventory BIGINT,

    -- Efficiency Ratios
    days_sales_outstanding DECIMAL(18,6),
    days_payables_outstanding DECIMAL(18,6),
    days_inventory_on_hand DECIMAL(18,6),
    receivables_turnover DECIMAL(18,6),
    payables_turnover DECIMAL(18,6),
    inventory_turnover DECIMAL(18,6),

    -- Graham Metrics
    graham_number DECIMAL(18,6),
    graham_net_net DECIMAL(18,6),

    -- System fields
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, date, period)
);

CREATE TABLE IF NOT EXISTS FMP_company_market_cap (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    market_cap BIGINT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, date)
);
CREATE TABLE IF NOT EXISTS alphavantage_SMA (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    sma DECIMAL(20,6),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sma (symbol, date)
);
CREATE TABLE IF NOT EXISTS alphavantage_EMA (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    ema DECIMAL(20,6),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sma (symbol, date)
);
CREATE TABLE IF NOT EXISTS alphavantage_BBANDS (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    upper_band DECIMAL(20,6),
    middle_band DECIMAL(20,6),
    lower_band DECIMAL(20,6),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sma (symbol, date)
)
