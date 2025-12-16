CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_time (
    date_key        INT PRIMARY KEY,     -- YYYYMMDD
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,   
    month_name      VARCHAR(20),
    week_of_year    INT NOT NULL,
    day_of_year     INT NOT NULL,
    day_of_week     INT NOT NULL,        -- Monday=0 ... Sunday=6 (Python convention)
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN NOT NULL,
    is_month_start  BOOLEAN NOT NULL,
    is_month_end    BOOLEAN NOT NULL,
    CONSTRAINT uq_date_key UNIQUE(date_key)
);

INSERT INTO dwh.dim_time (
    date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_year, day_of_week, day_name,
    is_weekend, is_month_start, is_month_end
)
SELECT
    -- date_key: YYYYMMDD
    (EXTRACT(YEAR FROM d)::INT * 10000 +
     EXTRACT(MONTH FROM d)::INT * 100 +
     EXTRACT(DAY FROM d)::INT)                          AS date_key,
    d                                                   AS full_date,
    EXTRACT(YEAR FROM d)::INT                           AS year,
    EXTRACT(QUARTER FROM d)::INT                        AS quarter,
    EXTRACT(MONTH FROM d)::INT                          AS month,
    TO_CHAR(d, 'Month')                                 AS month_name,
    EXTRACT(WEEK FROM d)::INT                           AS week_of_year,
    EXTRACT(DOY FROM d)::INT                            AS day_of_year,
    ((EXTRACT(DOW FROM d)::INT + 6) % 7)               AS day_of_week,  
    TO_CHAR(d, 'Day')                                   AS day_name,
    CASE
        WHEN ((EXTRACT(DOW FROM d)::INT + 6) % 7) >= 5 THEN TRUE
        ELSE FALSE
    END                                                 AS is_weekend,
    (d = date_trunc('month', d))                        AS is_month_start,
    (d = (date_trunc('month', d) + interval '1 month - 1 day')::date)
                                                        AS is_month_end
FROM generate_series('2010-01-01'::date, '2035-12-31'::date, '1 day') AS t(d)
ORDER BY d;

CREATE TABLE IF NOT EXISTS dwh.dim_company_informations (
    company_key          BIGSERIAL PRIMARY KEY,  
    symbol               VARCHAR(20) NOT NULL,   
    company_name         VARCHAR(255),
    cik                  VARCHAR(20),            
    isin                 VARCHAR(20),            
    cusip                VARCHAR(20),
    exchange_code        VARCHAR(50),
    exchange_full_name   VARCHAR(100),
    sector               VARCHAR(100),
    industry             VARCHAR(100),
    country              VARCHAR(50),
    ceo                  VARCHAR(100),
    full_time_employees  INT,                    
    address              VARCHAR(255),
    city                 VARCHAR(100),
    state                VARCHAR(50),
    zip                  VARCHAR(20),
    ipo_date             DATE,                   
    is_etf               BIGINT USING is_etf::INT,
    is_fund              BIGINT USING is_fund::INT,
    is_adr               BIGINT USING is_adr::INT,
    is_actively_trading  BIGINT USING is_actively_trading::INT,
    --SCD TYPE 2 COLUMNS (Các cột quản lý Lịch sử thay đổi)
    valid_from_date      DATE NOT NULL,         
    valid_to_date        DATE,                   
    is_current           BOOLEAN NOT NULL,       
    -- 5. AUDIT COLUMN (Cột Kiểm toán)
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Hạn chế (Constraints)
    CONSTRAINT uq_ticker_valid_from UNIQUE (ticker, valid_from_date)
);


CREATE TABLE IF NOT EXISTS dwh.fact_history_stock (
    -- 1. PRIMARY KEY (Khóa Chính)
    stock_fact_key       BIGSERIAL PRIMARY KEY,  
    -- 2. FOREIGN KEYS (Khóa Ngoại liên kết với các Dim Tables)
    company_key          BIGINT NOT NULL,
    date_key             INT NOT NULL,           
    open_price           DECIMAL(15, 4) NOT NULL,
    high_price           DECIMAL(15, 4),
    low_price            DECIMAL(15, 4),
    close_price          DECIMAL(15, 4) NOT NULL,
    volume               BIGINT NOT NULL,
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_company_date UNIQUE (company_key, date_key),
    -- Thiết lập Khóa Ngoại tham chiếu
    CONSTRAINT fk_company
        FOREIGN KEY (company_key) 
        REFERENCES dwh.dim_company_informations (company_key),  
    CONSTRAINT fk_date
        FOREIGN KEY (date_key) 
        REFERENCES dwh.dim_time (date_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_cash_flow (
    -- 1. PRIMARY KEY (Khóa Chính)
    cashflow_fact_key    BIGSERIAL PRIMARY KEY,  -- Khóa chính cho bảng Fact
    -- 2. FOREIGN KEYS (Khóa Ngoại liên kết với các Dim Tables)
    company_key          BIGINT NOT NULL,        -- Liên kết đến dim_company_informations (từ symbol/ticker)
    fiscal_date_key      INT NOT NULL,           -- Liên kết đến dim_time (từ fiscal_date_ending)
    reported_currency    VARCHAR(10),
    report_type_code     VARCHAR(10) NOT NULL,   
    -- 3. MEASURES (Các Giá trị Đo lường - Chuyển từ BIGINT sang DECIMAL)
    operating_cashflow                  DECIMAL(30, 4),
    payments_for_operating_activities   DECIMAL(30, 4),
    proceeds_from_operating_activities  DECIMAL(30, 4),
    change_in_operating_liabilities     DECIMAL(30, 4),
    change_in_operating_assets          DECIMAL(30, 4),
    depreciation_depletion_and_amortization DECIMAL(30, 4),
    change_in_receivables               DECIMAL(30, 4),
    change_in_inventory                 DECIMAL(30, 4),
    capital_expenditures                DECIMAL(30, 4),
    cashflow_from_investment            DECIMAL(30, 4),
    cashflow_from_financing             DECIMAL(30, 4),
    proceeds_from_repayments_of_short_term_debt DECIMAL(30, 4),
    payments_for_repurchase_of_common_stock DECIMAL(30, 4),
    payments_for_repurchase_of_equity   DECIMAL(30, 4),
    payments_for_repurchase_of_preferred_stock DECIMAL(30, 4),
    dividend_payout                     DECIMAL(30, 4),
    dividend_payout_common_stock        DECIMAL(30, 4),
    dividend_payout_preferred_stock     DECIMAL(30, 4),
    proceeds_from_issuance_of_common_stock DECIMAL(30, 4),
    proceeds_from_issuance_of_long_term DECIMAL(30, 4),
    proceeds_from_issuance_of_preferred_stock DECIMAL(30, 4),
    proceeds_from_repurchase_of_equity  DECIMAL(30, 4),
    proceeds_from_sale_of_treasury_stock DECIMAL(30, 4),
    change_in_cash_and_cash_equivalents DECIMAL(30, 4),
    change_in_exchange_rate             DECIMAL(30, 4),
    net_income                          DECIMAL(30, 4),
    -- 5. AUDIT COLUMN
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 6. CONSTRAINTS (Hạn chế)
    CONSTRAINT uq_cashflow_report_snapshot UNIQUE (company_key, fiscal_date_key, report_type_code),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES dwh.dim_company_informations (company_key),
    CONSTRAINT fk_fiscal_date FOREIGN KEY (fiscal_date_key) REFERENCES dwh.dim_time (date_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_balance_sheet (
    -- 1. PRIMARY KEY
    balance_sheet_fact_key BIGSERIAL PRIMARY KEY,
    -- 2. FOREIGN KEYS
    company_key          BIGINT NOT NULL,        
    fiscal_date_key      INT NOT NULL,      
    reported_currency    VARCHAR(10),     
    report_type_code     VARCHAR(10) NOT NULL,   
    -- Tài sản (Assets)
    total_assets                        DECIMAL(30, 4),
    total_current_assets                DECIMAL(30, 4),
    cash_and_cash_equivalents_at_carrying_value DECIMAL(30, 4),
    cash_and_short_term_investments     DECIMAL(30, 4),
    inventory                           DECIMAL(30, 4),
    current_net_receivables             DECIMAL(30, 4),
    total_non_current_assets            DECIMAL(30, 4),
    property_plant_equipment            DECIMAL(30, 4),
    accumulated_depreciation_amortization_ppe DECIMAL(30, 4),
    intangible_assets                   DECIMAL(30, 4),
    intangible_assets_excluding_goodwill DECIMAL(30, 4),
    goodwill                            DECIMAL(30, 4),
    investments                         DECIMAL(30, 4),
    long_term_investments               DECIMAL(30, 4),
    short_term_investments              DECIMAL(30, 4),
    other_current_assets                DECIMAL(30, 4),
    other_non_current_assets            DECIMAL(30, 4),
    -- Nợ phải trả (Liabilities)
    total_liabilities                   DECIMAL(30, 4),
    total_current_liabilities           DECIMAL(30, 4),
    current_accounts_payable            DECIMAL(30, 4),
    deferred_revenue                    DECIMAL(30, 4),
    current_debt                        DECIMAL(30, 4),
    short_term_debt                     DECIMAL(30, 4),
    total_non_current_liabilities       DECIMAL(30, 4),
    capital_lease_obligations           DECIMAL(30, 4),
    long_term_debt                      DECIMAL(30, 4),
    current_long_term_debt              DECIMAL(30, 4),
    long_term_debt_noncurrent           DECIMAL(30, 4),
    short_long_term_debt_total          DECIMAL(30, 4),
    other_current_liabilities           DECIMAL(30, 4),
    other_non_current_liabilities       DECIMAL(30, 4),
    -- Vốn chủ sở hữu (Equity)
    total_shareholder_equity            DECIMAL(30, 4),
    treasury_stock                      DECIMAL(30, 4),
    retained_earnings                   DECIMAL(30, 4),
    common_stock                        DECIMAL(30, 4),
    common_stock_shares_outstanding     DECIMAL(30, 4),
    -- 5. AUDIT COLUMN
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 6. CONSTRAINTS
    CONSTRAINT uq_balance_sheet_snapshot UNIQUE (company_key, fiscal_date_key, report_type_code),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES dwh.dim_company_informations (company_key),
    CONSTRAINT fk_fiscal_date FOREIGN KEY (fiscal_date_key) REFERENCES dwh.dim_time (date_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_income_statement (
    -- 1. PRIMARY KEY
    income_statement_fact_key BIGSERIAL PRIMARY KEY,
    -- 2. FOREIGN KEYS
    company_key          BIGINT NOT NULL,        
    fiscal_date_key      INT NOT NULL,           
    reported_currency    VARCHAR(10),
    report_type_code     VARCHAR(10) NOT NULL,   
    total_revenue                       DECIMAL(30, 4),
    gross_profit                        DECIMAL(30, 4),
    cost_of_revenue                     DECIMAL(30, 4),
    cost_of_goods_and_services_sold     DECIMAL(30, 4),
    -- Chi phí hoạt động
    operating_income                    DECIMAL(30, 4),
    selling_general_and_administrative  DECIMAL(30, 4),
    research_and_development            DECIMAL(30, 4),
    operating_expenses                  DECIMAL(30, 4),
    -- Thu nhập và chi phí khác
    investment_income_net               DECIMAL(30, 4),
    net_interest_income                 DECIMAL(30, 4),
    interest_income                     DECIMAL(30, 4),
    interest_expense                    DECIMAL(30, 4),
    non_interest_income                 DECIMAL(30, 4),
    other_non_operating_income          DECIMAL(30, 4),
    depreciation                        DECIMAL(30, 4),
    depreciation_and_amortization       DECIMAL(30, 4),
    -- Thu nhập trước và sau thuế
    income_before_tax                   DECIMAL(30, 4),
    income_tax_expense                  DECIMAL(30, 4),
    interest_and_debt_expense           DECIMAL(30, 4),
    net_income_from_continuing_operations DECIMAL(30, 4),
    comprehensive_income_net_of_tax     DECIMAL(30, 4),
    -- Các chỉ tiêu hiệu quả
    ebit                                DECIMAL(30, 4),
    ebitda                              DECIMAL(30, 4),
    net_income                          DECIMAL(30, 4),
    -- 5. AUDIT COLUMN
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 6. CONSTRAINTS
    CONSTRAINT uq_income_statement_snapshot UNIQUE (company_key, fiscal_date_key, report_type_code),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES dwh.dim_company_informations (company_key),
    CONSTRAINT fk_fiscal_date FOREIGN KEY (fiscal_date_key) REFERENCES dwh.dim_time (date_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_earnings (
    -- 1. PRIMARY KEY
    earnings_fact_key    BIGSERIAL PRIMARY KEY,
    company_key          BIGINT NOT NULL,        -- Liên kết đến dim_company_informations (từ symbol/ticker)
    fiscal_date_key      INT NOT NULL,           -- Liên kết đến dim_time (từ fiscal_date_ending)
    reported_date_key    INT,                    -- Liên kết đến dim_time (từ reported_date)
    report_type_code     VARCHAR(10) NOT NULL,   -- 'annual' hoặc 'quarterly'
    reported_eps         DECIMAL(20, 6),         -- Thu nhập thực tế trên mỗi cổ phiếu
    estimated_eps        DECIMAL(20, 6),         -- Thu nhập ước tính trên mỗi cổ phiếu (dự báo)
    surprise             DECIMAL(20, 6),         -- Chênh lệch tuyệt đối (Reported - Estimated)
    surprise_percentage  DECIMAL(10, 6),         -- Phần trăm bất ngờ
    reported_time        VARCHAR(50),            
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_earnings_snapshot UNIQUE (company_key, fiscal_date_key, report_type_code),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES dwh.dim_company_informations (company_key),
    CONSTRAINT fk_fiscal_date FOREIGN KEY (fiscal_date_key) REFERENCES dwh.dim_time (date_key),
    CONSTRAINT fk_reported_date FOREIGN KEY (reported_date_key) REFERENCES dwh.dim_time (date_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_market_cap (
    market_cap_fact_key  BIGSERIAL PRIMARY KEY,
    company_key          BIGINT NOT NULL,        -- Liên kết đến dim_company_informations (từ symbol)
    date_key             INT NOT NULL,           -- Liên kết đến dim_time (từ date)
    market_cap           DECIMAL(30, 4),         -- Chuyển BIGINT sang DECIMAL để xử lý số lớn và cho phép tính toán chính xác hơn
    dw_load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_market_cap_daily UNIQUE (company_key, date_key),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES dwh.dim_company_informations (company_key),
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dwh.dim_time (date_key)
);