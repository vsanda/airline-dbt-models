-- models/fct/fct_crew_costs.sql

with crew as (
    select * from {{ ref('int_crew_metrics') }}
),

flight_context as (
    select
        flight_id,
        aircraft_id,
        route_id
    from {{ ref('stg_flights') }}
)

select
    c.flight_id,
    f.route_id,
    f.aircraft_id,
    c.num_crew,
    c.total_crew_hours,
    c.avg_hourly_rate_usd,
    c.crew_cost_total_usd
from crew c
left join flight_context f on c.flight_id = f.flight_id
