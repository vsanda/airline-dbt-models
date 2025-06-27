with source as (
    select * from {{ source('raw_airline', 'supplier_logs') }}
),

cleaned as (
    select
        order_id,
        supplier_id,
        supplier,
        lower(service_type) as service_type,
        aircraft_id,
        cast(order_date as date) as order_date,
        cast(expected_delivery as date) as expected_delivery,
        lower(status) as delivery_status,
        {{ dbt.safe_cast("cost_usd", "float") }} as cost_usd,
        flight_id,
        cast(flight_day as date) as flight_day,
        cast(recorded_at as timestamp) as recorded_at
    from source
)



select * from cleaned
