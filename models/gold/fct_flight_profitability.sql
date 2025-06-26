-- fct_flight_profitability.sql

with cost as (
    select
        flight_id,
        flight_day,
        route_id,
        flight_status as status,
        total_cost_usd
    from {{ ref('int_flight_metrics') }}
),

revenue as (
    select
        flight_id,
        total_revenue_usd
    from {{ ref('int_flight_revenue') }}
)

select
    c.flight_id,
    c.flight_day,
    c.route_id,
    c.status,
    sum(r.total_revenue_usd) as total_revenue_usd,
    sum(c.total_cost_usd) as total_cost_usd,
    sum(round(coalesce(r.total_revenue_usd, 0) - coalesce(c.total_cost_usd, 0), 2)) as profit_margin_usd
from cost c
inner join revenue r on c.flight_id = r.flight_id
group by c.flight_id,c.flight_day,c.route_id,c.status
