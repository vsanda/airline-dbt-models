-- models/gold/fct_fuel_costs.sql

select
    flight_id,
    fuel_price_pg,
    est_gallons,
    (fuel_price_pg * est_gallons) as total_fuel_usd
from {{ ref('int_fuel_metrics') }}
where
    est_gallons is not null
    and fuel_price_pg is not null
