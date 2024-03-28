{{ config(materialized='table') }} 

select town, street_name, `block`
from {{ ref('stg_sales') }} -- Use the `ref` function to select from other models
group by town, street_name, `block`;

