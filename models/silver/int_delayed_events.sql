-- int_ontime_events.sql

with flights as (
    select
        flight_id,
        flight_day,
        flight_status,
        delay_minutes,
        delay_cost_usd,
        route_id
    from {{ ref('stg_flights') }}
),

-- crew_flags as (
--     select
--         flight_id,
--         bool_or(crew_flagged) as crew_flag
--     from {{ ref('stg_crew_payroll') }}
--     where flight_id is not null
--     group by flight_id
-- ),

supplier_flags as (
    select
        flight_id,
        min(supplier_id) as supplier_id
    from {{ ref('stg_supplier_logs') }}
    where flight_id is not null
    group by flight_id
)

select
    f.flight_id,
    f.flight_day,
    f.flight_status,
    f.delay_minutes,
    f.delay_cost_usd,
    f.route_id,
    s.supplier_id
from flights f
left join supplier_flags s on f.flight_id = s.flight_id
-- left join crew_flags c on f.flight_id = c.flight_id
