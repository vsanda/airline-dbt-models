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
        timestamp as ingestion_time
    from source
)

select * from cleaned
