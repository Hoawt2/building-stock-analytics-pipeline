
    
    

select
    stock_price_id as unique_field,
    count(*) as n_records

from "de_psql"."core"."fact_stock_price"
where stock_price_id is not null
group by stock_price_id
having count(*) > 1


