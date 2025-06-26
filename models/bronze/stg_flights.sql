with source as (
    select * from {{ source('raw_airline', 'flights') }}
),

cleaned as (
    select
        icao24,
        trim(callsign) as callsign,
        origin_country,
        to_timestamp(time_position) as time_position,
        to_timestamp(last_contact) as last_contact,
        {{ dbt.safe_cast("longitude", "float") }} as longitude,
        {{ dbt.safe_cast("latitude", "float") }} as latitude,
        {{ dbt.safe_cast("baro_altitude", "float") }} as baro_altitude,
        on_ground::boolean as is_on_ground,
        {{ dbt.safe_cast("velocity", "float") }} as velocity_knots,
        {{ dbt.safe_cast("heading", "float") }} as heading_deg,
        {{ dbt.safe_cast("vertical_rate", "float") }} as vertical_rate_fpm,
        {{ dbt.safe_cast("geo_altitude", "float") }} as geo_altitude,
        squawk,
        spi::boolean as is_spi,
        position_source,
        
        -- Enriched fields from new columns
        flight_id,
        cast(departure_time as timestamp) as departure_time,
        cast(arrival_time as timestamp) as arrival_time,
        origin_airport_code,
        destination_airport_code,
        route_id,
        lower(status) as flight_status,
        {{ dbt.safe_cast("delay_minutes", "int") }} as delay_minutes,
        {{ dbt.safe_cast("delay_cost", "float") }} as delay_cost_usd,
        aircraft_id,
        cast(flight_day as date) as flight_day,
        timestamp as ingestion_time
        
    from source
)

select * from cleaned
