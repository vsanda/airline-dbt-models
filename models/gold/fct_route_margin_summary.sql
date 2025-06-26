-- fct_route_margin_summary.sql

with flight_profit as (
    select
        route_id,
        total_revenue_usd,
        total_cost_usd,
        profit_margin_usd
    from {{ ref('fct_flight_profitability') }}
    where route_id is not null
),

aggregated as (
    select
        route_id,
        round(avg(total_revenue_usd), 2) as avg_revenue_usd,
        round(avg(total_cost_usd), 2) as avg_cost_usd,
        round(avg(profit_margin_usd), 2) as avg_margin_usd,
        count(*) as num_flights
    from flight_profit
    group by route_id
),

route_names as (
    select
        route_id,
        origin_country as origin,
        destination_country as destination
    from {{ ref('int_route_lookup') }}
)

select
    a.route_id,
    r.origin,
    r.destination,
    a.avg_revenue_usd,
    a.avg_cost_usd,
    a.avg_margin_usd
    -- a.num_flights
from aggregated a
left join route_names r on a.route_id = r.route_id
