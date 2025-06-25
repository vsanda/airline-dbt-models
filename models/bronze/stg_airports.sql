with source as (
    select * from {{ source('raw_airline', 'airports_data') }}
),

cleaned as (
    select
        airport_code,
        trim(name) as airport_name,
        trim(city) as city,
        trim(country) as country,
        timezone,
        {{ dbt.safe_cast("elevation_ft", "int") }} as elevation_ft,
        date::date as ingestion_date
    from source
)

select * from cleaned
