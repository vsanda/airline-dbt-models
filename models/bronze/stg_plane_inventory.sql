with source as (
    select * from {{ source('raw_airline', 'inventory_data') }}
),

cleaned as (
    select
        aircraft_id,
        upper(model) as aircraft_model,
        initcap(manufacturer) as manufacturer,
        {{ dbt.safe_cast("capacity", "int") }} as seat_capacity,
        lower(status) as aircraft_status,
        {{ dbt.safe_cast("date", "date") }} as ingestion_date
    from source
)

select * from cleaned
