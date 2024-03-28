{% set current_date = 'active' %}  -- Define a variable

with renamed_source as
(select
    `sale_id` as sale_id,
    `block` as block_number,
    `street_name` as street_name,
    `town` as town_name,
    `flat_type` as flat_type,

    cast(`floor_area_sqm` as decimal) as floor_area_sqm,
    cast(`resale_price` as decimal) as price_sold,

    year(str_to_date(`month`, '%Y-%m')) as year_sold,
    month(str_to_date(`month`, '%Y-%m')) as month_sold

from {{ source('hdb', 'raw_sales')}}
)

select *
from renamed_source