with transform_dtypes as
(select
    _id as id
    `month` as resale_date,
    town,
    flat_type,
    `block` as block_number,
    street_name,
    storey_range,
    flat_model,
    year(str_to_date(`lease_commence_date`, '%Y')) as lease_commence_year,
    cast(substr(remaining_lease, 1, 2) as int) as remaining_lease_years,
    cast(`floor_area_sqm` as decimal) as floor_area_sqm,
    cast(`resale_price` as decimal) as price_sold

from {{ source('raw', 'resale_prices')}}
)

select *
from transform_dtypes