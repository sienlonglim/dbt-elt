{% set status = 'active' %}  -- Define a variable

with source as
(select *
from {{ source('hdb_prices_dev', 'raw_sales')}})