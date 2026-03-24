select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    stock_price_id as unique_field,
    count(*) as n_records

from "de_psql"."core"."fact_stock_price"
where stock_price_id is not null
group by stock_price_id
having count(*) > 1



      
    ) dbt_internal_test