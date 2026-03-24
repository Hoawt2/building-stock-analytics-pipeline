{% snapshot snp_company %}

{{
    config(
      target_schema='core',
      unique_key='company_info_id',
      strategy='check',
      check_cols='all'
    )
}}

SELECT * FROM {{ ref('stg_company_information') }}

{% endsnapshot %}
