{{ config(materialized='table') }} -- Materialized as table

select *
from {{ ref('stg_sales')}}

{% if target.name!='prod' %} -- dev
    where resale_date like "2023-%"
{% else %} -- prod

{% endif %}


