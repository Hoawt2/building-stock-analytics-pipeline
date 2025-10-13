CREATE TABLE Company (
    company_id INT PRIMARY KEY AUTO_INCREMENT,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(50),
    industry VARCHAR(100),
    country VARCHAR(50),
    ipo_year INT
);


CREATE TABLE Stock (
    stock_id INT PRIMARY KEY AUTO_INCREMENT,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    company_id INT NOT NULL,
    exchange_id INT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES Company(company_id),
    
);

CREATE TABLE Stock_Price_History (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stock_id INT NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(15,2),
    high_price DECIMAL(15,2),
    low_price DECIMAL(15,2),
    close_price DECIMAL(15,2),
    last_price DECIMAL(15,2),
    volume BIGINT,
    market_cap DECIMAL(20,2),
    FOREIGN KEY (stock_id) REFERENCES Stock(stock_id)
);
CREATE TABLE Company_Financials (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    report_date DATE NOT NULL,
    revenue DECIMAL(15,2),
    net_income DECIMAL(15,2),
    total_assets DECIMAL(15,2),
    total_liabilities DECIMAL(15,2),
    operating_cash_flow DECIMAL(15,2),
    eps DECIMAL(10,2),
    currency VARCHAR(10),
    FOREIGN KEY (company_id) REFERENCES Company(company_id)
);
CREATE TABLE Financial_Ratios (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    company_id INT NOT NULL,
    report_date DATE NOT NULL,
    current_ratio DECIMAL(10,2),
    quick_ratio DECIMAL(10,2),
    roa DECIMAL(10,2),
    roe DECIMAL(10,2),
    debt_to_equity DECIMAL(10,2),
    pe_ratio DECIMAL(10,2),
    pb_ratio DECIMAL(10,2),
    dividend_yield DECIMAL(10,2),
    interest_coverage_ratio DECIMAL(10,2),
    ebitda_margin DECIMAL(10,2),
    FOREIGN KEY (company_id) REFERENCES Company(company_id)
);
