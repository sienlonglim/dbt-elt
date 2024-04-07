select 
    town, 
    street_name, 
    `block_number`, 
    concat(`block_number`, ", ", street_name) as full_address
from {{ ref('stg_sales') }} -- Use the `ref` function to select from other models
group by town, street_name, `block_number`;

