with source as (
    select * from {{ source('raw_airline', 'supplier_logs') }}
),

cleaned as (
    select
        initcap(supplier) as supplier_name,
        lower(part) as part_type,
        {{ dbt.safe_cast("order_date", "date") }} as order_date,
        {{ dbt.safe_cast("expected_delivery", "date") }} as expected_delivery,
        lower(status) as delivery_status,
        {{ dbt.safe_cast("delay_days", "integer") }} as delay_days,
        case
            when lower(status) in ('delayed', 'backordered') and {{ dbt.safe_cast("delay_days", "integer") }} > 0 then true
            else false
        end as is_delayed
    from source
)

select * from cleaned
