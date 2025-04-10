SET foreign_key_checks = 0;

DROP DATABASE IF EXISTS stock_bi;
CREATE DATABASE stock_bi;
USE stock_bi;

DROP TABLE IF EXISTS fact_company_financials_quarterly;
DROP TABLE IF EXISTS fact_stock_history;
DROP TABLE IF EXISTS fact_index_history;
DROP TABLE IF EXISTS fact_financial_ratios;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_stock;
DROP TABLE IF EXISTS dim_index;

SET foreign_key_checks = 1;

-- Tạo bảng dimension
CREATE TABLE dim_index (
    index_id INT PRIMARY KEY,
    index_name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) UNIQUE,
    calculation_method VARCHAR(255)
);

CREATE TABLE dim_stock (
    stock_id INT PRIMARY KEY ,
    ticker VARCHAR(10) UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(50),
    industry VARCHAR(100),
    country VARCHAR(50),
    ipo_year INT
);

CREATE TABLE dim_time (
    date_key INT PRIMARY KEY,  -- Định dạng yyyymmdd
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend TINYINT(1) NOT NULL
);

DELIMITER $$

CREATE PROCEDURE fill_dim_time()
BEGIN
    DECLARE start_date DATE;
    DECLARE end_date DATE;
    
    SET start_date = '2020-01-01';
    SET end_date = '2025-12-31';
    
    WHILE start_date <= end_date DO
        INSERT INTO dim_time (
            date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend
        ) VALUES (
            DATE_FORMAT(start_date, '%Y%m%d'),
            start_date,
            YEAR(start_date),
            QUARTER(start_date),
            MONTH(start_date),
            MONTHNAME(start_date),
            DAY(start_date),
            DAYOFWEEK(start_date),
            DAYNAME(start_date),
            WEEK(start_date, 3),
            CASE WHEN DAYOFWEEK(start_date) IN (1, 7) THEN 1 ELSE 0 END
        );
        
        SET start_date = DATE_ADD(start_date, INTERVAL 1 DAY);
    END WHILE;
END$$

DELIMITER ;

CALL fill_dim_time();

-- Bảng fact lưu lịch sử chỉ số chứng khoán
CREATE TABLE fact_index_history (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    index_id INT NOT NULL,
    time_id INT NOT NULL,
    open_value DECIMAL(10,2) NOT NULL,
    low_value DECIMAL(10,2),
    high_value DECIMAL(10,2),
    `last_value` DECIMAL(10,2) NOT NULL,
    daily_return DECIMAL(10,4),
    market_volatility DECIMAL(10,4),
    FOREIGN KEY (index_id) REFERENCES dim_index(index_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(date_key)
);

-- Bảng fact lưu lịch sử giá cổ phiếu
CREATE TABLE fact_stock_history (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    time_id INT NOT NULL,
    open_price DECIMAL(15,2) NOT NULL,
    high_price DECIMAL(15,2),
    low_price DECIMAL(15,2),
    last_price DECIMAL(15,2),
    close_price DECIMAL(15,2) NOT NULL,
    volume BIGINT NOT NULL,
    market_cap DECIMAL(20,2),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(date_key)
);

-- Bảng lưu dữ liệu tài chính theo từng quý
CREATE TABLE fact_company_financials_quarterly (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    time_id INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    revenue DECIMAL(15,2) NOT NULL, -- Doanh thu
    net_income DECIMAL(15,2) NOT NULL, -- Lợi nhuận ròng
    total_assets DECIMAL(15,2) NOT NULL, -- Tổng tài sản
    total_liabilities DECIMAL(15,2) NOT NULL, -- Tổng nợ phải trả
    operating_cash_flow DECIMAL(15,2), -- Dòng tiền từ HĐKD
    eps DECIMAL(10,2), -- Lợi nhuận trên cổ phiếu
    currency VARCHAR(10) DEFAULT 'USD',
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(date_key)
);

-- Bảng lưu các tỷ lệ tài chính quan trọng
CREATE TABLE fact_financial_ratios (
    fact_id INT PRIMARY KEY AUTO_INCREMENT,
    stock_id INT NOT NULL,
    time_id INT NOT NULL,
    current_ratio DECIMAL(10,2),
    roe DECIMAL(10,2),
    debt_to_equity DECIMAL(10,2),
    pe_ratio DECIMAL(10,2),
    pb_ratio DECIMAL(10,2),
    dividend_yield DECIMAL(10,2),
    interest_coverage_ratio DECIMAL(10,2),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(date_key)
);
