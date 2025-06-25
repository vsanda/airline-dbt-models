with flights as (
    select * from {{ ref('stg_flights') }}
),

airports as (
    select * from {{ ref('stg_airports') }}
),

joined as (
    select
        f.icao24 as airport_id,
        f.callsign,
        f.origin_country,
        f.longitude,
        f.latitude,
        f.geo_altitude,
        f.velocity_knots,
        f.heading_deg,
        f.ingestion_time,
        a.city as nearest_airport_city,
        a.country as airport_country,
        a.airport_code,
        a.timezone,
        -- This is a placeholder logic assuming origin_country = airport_country means domestic
        case 
            when f.origin_country = a.country then true 
            else false 
        end as is_domestic,
        -- Mocked flight route, can be used later for grouping
        f.origin_country || ' → ' || a.country as route_label
    from flights f
    left join airports a
        on f.origin_country = a.country
)

select * from joined
