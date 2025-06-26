-- models/silver/dim_route_lookup.sql

with base as (
    select
        route_id,
        origin_airport_code,
        destination_airport_code,
        min(departure_time) as first_flight_time,
        max(departure_time) as last_flight_time,
        count(distinct flight_id) as total_flights,
        round(avg(extract(epoch from (arrival_time - departure_time)) / 60),2) as avg_flight_duration_minutes
    from {{ ref('stg_flights') }}
    where route_id is not null
      and origin_airport_code != destination_airport_code
    group by route_id, origin_airport_code, destination_airport_code
)
,
with_origin_country as (
    select
        oa.country as origin_country,
        b.*
    from base b
    left join {{ ref('stg_airports') }} oa
      on b.origin_airport_code = oa.airport_code
),

with_destination_country as (
    select
        o.*,
        da.country as destination_country
    from with_origin_country o
    left join {{ ref('stg_airports') }} da
      on o.destination_airport_code = da.airport_code
)

select * from with_destination_country

