{{ config(materialized='table') }} -- This overwrites the dbt_project settings

with renamed as
(select 
    ----------  ids
    sale_id as sale_id,

    ---------- strings
    town as town,
    street_name as street_name,
    `block` as block_number,
    flat_type as flat_type,

    ---------- numerics
    floor_area_sqm::float as floor_area_sqm,
    resale_price::float as price_sold,

    ---------- booleans
    ---------- dates
    date(`month`) as month_at

    ---------- timestamps


from {{ ref('stg_sales')}})

select *
from source;
