{{ config(materialized='table') }} -- This overwrites the dbt_project settings

with source as
(select *
from hdb_prices.raw_sales)

select *
from source;
