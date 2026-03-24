WITH snapshot AS (
    -- Kế thừa từ bảng Snapshot lịch sử ta vừa đẻ ra (dbt sẽ tự hiểu nó từ bảng snapshot)
    SELECT * FROM "de_psql"."core"."snp_company"
)

SELECT
    dbt_scd_id AS company_key,
    company_info_id AS company_id,  -- Đây là khóa chính 
    symbol,
    company_name,
	cik_code,
	isin_code,
	cusip_code,
	exchange_full_name,
	exchange_code,
	industry,
	sector,
	current_price,
	market_cap,
	beta,
	last_dividend,
	change_value,
	change_percentage,
	volume,
	average_volume,
	price_range,
	currency,
	ceo,
	website,
	description,
	country,
	state,
	city,
	address,
	zip_code,
	phone,
	full_time_employees,
	is_etf,
	is_actively_trading,
	is_adr,
	is_fund,
	ipo_date,
	image_url,
	default_image,
	source_load_timestamp,
	dbt_processed_at,
    -- Viết lại cho đẹp các cột SCD Type 2 để tiện track dòng thời gian
    -- TRICK đỉnh cao: Ép dòng đầu tiên của mỗi công ty lùi về quá khứ xa xôi (1900-01-01) để Join không bị hụt data cũ!
    CASE 
        WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY symbol) THEN '1900-01-01'::timestamp
        ELSE dbt_valid_from 
    END AS valid_from,
    COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM snapshot