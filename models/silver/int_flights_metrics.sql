-- models/silver/slv_flights.sql

with flights as (
    select * from {{ ref('stg_flights') }}
),

aircraft as (
    select 
        aircraft_id,
        aircraft_type,
        model,
        fuel_efficiency_gph
    from {{ ref('stg_aircraft') }}
),

cleaned as (
    select
        f.flight_id,
        f.route_id,

        -- Route fields from stg_flights
        f.origin_airport_code,
        f.destination_airport_code,
        f.distance_miles,
        f.region,
        f.route_category,
        f.departure_time,
        f.arrival_time,
        f.flight_day,
        f.aircraft_id,
        a.aircraft_type,
        a.model as aircraft_model,
        a.fuel_efficiency_gph,
        f.flight_status,
        f.delay_minutes,
        -- Metadata
        f.ingestion_time

    from flights f
    left join aircraft a on f.aircraft_id = a.aircraft_id
)

select * from cleaned
