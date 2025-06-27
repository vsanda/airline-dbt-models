with base as (

    select
        flight_id,
        fuel_burn_rate_gph,
        flight_hours,
        fuel_price_per_gal,
        fuel_cost
    from {{ ref('int_fuel_metrics') }}

)

select
    flight_id,                   
    fuel_burn_rate_gph,          
    flight_hours,             
    fuel_price_per_gal,          
    round(fuel_cost, 2) as fuel_cost  
from base
