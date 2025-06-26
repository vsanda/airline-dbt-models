-- fct_supplier_costs.sql

with supplier_silver as (
    select *
    from {{ ref('int_supplier_metrics') }}
),

by_month as (
    select
        supplier_id,
        count(*) as num_orders,
        avg(avg_delay_days) as avg_delay_days,
        sum(case when lower(delivery_status) in ('delayed', 'backordered') then 1 else 0 end)::float / count(*) as backorder_rate,
        sum(total_cost_usd) as total_cost_usd
    from supplier_silver
    group by supplier_id
),

with_flag as (
    select
        *,
        round(avg_delay_days * 500, 2) as delay_cost_est,
        case
            when avg_delay_days > 5 or backorder_rate > 0.25 then 'bad'
            when backorder_rate between 0.1 and 0.25 then 'watchlist'
            else 'good'
        end as status_flag
    from by_month
    order by supplier_id
)

select * from with_flag

