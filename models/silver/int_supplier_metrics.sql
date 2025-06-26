-- silver_supplier_metrics.sql

with supplier_base as (
    select
        supplier_id,
        lower(supplier_category) as category,
        delivery_status,
        count(*) as num_orders,
        avg(delay_days) as avg_delay_days,
        sum(case when lower(delivery_status) in ('delayed', 'backordered') then 1 else 0 end) * 1.0 / count(*) as backorder_rate,
        sum(part_cost_usd) as total_cost_usd
    from {{ ref('stg_supplier_logs') }}
    where supplier_id is not null
    group by supplier_id, lower(supplier_category), delivery_status
),

supplier_flagged as (
    select
        *,
        case
            when avg_delay_days > 5 or backorder_rate > 0.25 then 'bad'
            when backorder_rate between 0.1 and 0.25 then 'watchlist'
            else 'good'
        end as status_flag,
        round(avg_delay_days * 500, 2) as delay_cost_est
    from supplier_base
)

select * from supplier_flagged
