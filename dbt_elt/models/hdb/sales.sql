{{ config(materialized='table') }} -- This overwrites the dbt_project settings

select *
from {{ ref('stg_sales')}}

