with source as (
    select * from {{ source('raw_airline', 'inventory_data') }}
),

cleaned as (
    select
        aircraft_id,
        upper(model) as aircraft_model,
        initcap(manufacturer) as manufacturer,
        aircraft_type,
        {{ dbt.safe_cast("year_built", "int") }} as year_built,
        {{ dbt.safe_cast("capacity", "int") }} as seat_capacity,
        lower(status) as aircraft_status,
        fuel_efficiency_gph,
        case
            when lower(status) = 'maintenance' then true
            else false
        end as is_under_maintenance,
        {{ dbt.safe_cast("date", "date") }} as ingestion_date
    from source
)

select * from cleaned
