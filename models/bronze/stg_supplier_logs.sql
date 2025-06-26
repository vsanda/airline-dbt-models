with source as (
    select * from {{ source('raw_airline', 'supplier_logs') }}
),

cleaned as (
    select
        order_id,
        supplier_id,
        initcap(supplier) as supplier_name,
        lower(category) as supplier_category,
        lower(part) as part_type,
        {{ dbt.safe_cast("order_date", "date") }} as order_date,
        {{ dbt.safe_cast("expected_delivery", "date") }} as expected_delivery,
        lower(status) as delivery_status,
        {{ dbt.safe_cast("delay_days", "int") }} as delay_days,
        {{ dbt.safe_cast("cost_usd", "numeric") }} as part_cost_usd,
        flight_id,
        {{ dbt.safe_cast("flight_day", "date") }} as flight_day,
        {{ dbt.safe_cast("recorded_at", "timestamp") }} as ingestion_timestamp,
        case
            when lower(status) in ('delayed', 'backordered') and {{ dbt.safe_cast("delay_days", "int") }} > 0 then true
            else false
        end as is_delayed
    from source
)


select * from cleaned
