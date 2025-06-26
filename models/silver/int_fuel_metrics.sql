with flights as (
    select
        flight_id,
        aircraft_id,
        cast(departure_time as date) as flight_day,
        extract(epoch from (cast(arrival_time as timestamp) - cast(departure_time as timestamp))) / 3600.0 as flight_duration_hours
    from {{ ref('stg_flights') }}
),

aircraft as (
    select
        aircraft_id,
        fuel_efficiency_gph
    from {{ ref('stg_plane_inventory') }}
),

fuel_prices as (
    select
        price_month,
        price_per_gallon_usd
    from {{ ref('stg_fuel_prices') }}
    where product_name = 'Kerosene-Type Jet Fuel'
    union all
    select
        date '2025-06-01' as price_month,
        3.25 as price_per_gallon_usd    -- Placeholder for future price
),

joined as (
    select
        f.flight_id,
        f.flight_day,
        f.aircraft_id,
        f.flight_duration_hours,
        a.fuel_efficiency_gph,
        fp.price_per_gallon_usd as fuel_price_pg
    from flights f
    join aircraft a
        on f.aircraft_id = a.aircraft_id
    left join fuel_prices fp
        on date_trunc('month', f.flight_day) = fp.price_month
    where a.fuel_efficiency_gph is not null
)

select
    flight_id,
    aircraft_id,
    flight_day,
    flight_duration_hours,
    fuel_efficiency_gph,
    fuel_price_pg,
    (flight_duration_hours * fuel_efficiency_gph) as est_gallons
from joined
where flight_duration_hours is not null
  and fuel_efficiency_gph is not null
