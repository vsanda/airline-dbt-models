-- models/gold/fct_route_margin_summary.sql

with flight_summary as (
    select
        f.flight_id,
        r.route_id,
        r.origin_airport_code,
        r.destination_airport_code,
        f.revenue_total,
        f.fuel_cost,
        f.crew_cost,
        f.supplier_cost,
        f.delay_cost,
        f.fixed_cost,
        f.total_cost,
        f.net_profit,
        f.gross_profit,
        f.profit_pct,
        d.delay_minutes
    from {{ ref('profitability_summary') }} f
    left join {{ ref('int_flights_metrics') }} r
        on f.flight_id = r.flight_id
    left join {{ ref('int_delayed_events') }} d
        on f.flight_id = d.flight_id
)

select
    route_id,
    max(origin_airport_code) as origin,
    max(destination_airport_code) as destination,
    count(*) as num_flights,
    round(avg(revenue_total), 2) as avg_revenue,
    round(avg(total_cost), 2) as avg_cost,
    round(avg(net_profit), 2) as avg_margin,
    round(avg(profit_pct), 2) as avg_profit_pct,
    sum(case when delay_minutes::numeric > 0 then 1 else 0 end) as delay_count,
    round(avg(delay_minutes) filter (where delay_minutes > 0), 2) as avg_delay_minutes
from flight_summary
group by route_id
