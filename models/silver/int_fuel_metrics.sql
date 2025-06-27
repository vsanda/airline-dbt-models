with flights as (
    select 
        flight_id,
        aircraft_id,
        route_id,
        flight_day,
        departure_time,
        arrival_time,
        round(extract(epoch from (arrival_time - departure_time)) / 3600.0, 2) as flight_hours
    from {{ ref('int_flights_metrics') }}
),

aircraft as (
    select 
        aircraft_id,
        fuel_efficiency_gph as fuel_burn_rate_gph
    from {{ ref('stg_aircraft') }}
),

fuel_price as (
    select
        price_month_date,
        price_per_gallon_usd as fuel_price_per_gal
    from {{ ref('stg_fuel_prices') }}
    where fuel_category = 'Jet Fuel'  -- adjust if needed
),

joined as (
    select 
        f.flight_id,
        f.aircraft_id,
        a.fuel_burn_rate_gph,
        f.flight_hours,
        p.price_month_date,
        p.fuel_price_per_gal,
        (a.fuel_burn_rate_gph * f.flight_hours * p.fuel_price_per_gal)::numeric(10,2) as fuel_cost
    from flights f
    left join aircraft a on f.aircraft_id = a.aircraft_id
    left join fuel_price p on date_trunc('month', f.flight_day) = date_trunc('month', p.price_month_date)
)

select * from joined
