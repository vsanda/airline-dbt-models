with source as (
    select * from {{ source('raw_airline', 'inventory_data') }}
),

cleaned as (
    select
        aircraft_id,
        model,
        manufacturer,
        lower(aircraft_type) as aircraft_type,
        cast(year_built as int) as year_built,
        cast(capacity as int) as max_capacity,
        lower(status) as aircraft_status,
        {{ dbt.safe_cast("fuel_efficiency_gph", "float") }} as fuel_efficiency_gph,
        cast(date as date) as inventory_date
    from source
)

select * from cleaned
