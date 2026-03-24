
      
  
    

  create  table "de_psql"."core"."snp_company"
  
  
    as
  
  (
    

    select *,
        md5(coalesce(cast(company_info_id as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        nullif(now()::timestamp without time zone, now()::timestamp without time zone) as dbt_valid_to
    from (
        



SELECT * FROM "de_psql"."public"."stg_company_information"

    ) sbq



  );
  
  