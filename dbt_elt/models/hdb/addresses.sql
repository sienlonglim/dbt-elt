{{ config(materialized='table') }} 

select town_name, street_name, `block_number`
from {{ ref('stg_sales') }} -- Use the `ref` function to select from other models
group by town_name, street_name, `block_number`;

