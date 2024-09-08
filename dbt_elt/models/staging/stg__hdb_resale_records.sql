with hdb_resale_records as (select* from {{ source('raw', 'raw__hdb_resale_records')}} ),

final as (

    select
        _id::int as id,
        cast(strptime(month, '%Y-%m') as date) as resale_year_month,
        -- Varchars
        lower(town) as town,
        case 
            when substring(flat_type, 1, 1) in ('1', '2', '3', '4', '5')
                then substring(flat_type, 1, 1)::decimal
            else 5.5
            end as flat_type,
        block as block_number,
        lower(street_name) as street_name,
        lower(flat_model) as flat_model,
        -- Numericals
        storey_range,
        lease_commence_date::int as lease_commence_year,
        substring(remaining_lease, 1, 2)::int as remaining_lease_years,
        floor_area_sqm::decimal as floor_area_sqm,
        resale_price:: decimal as resale_price
    from hdb_resale_records

)

select * from final