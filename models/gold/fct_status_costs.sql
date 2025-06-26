-- fact_delayed_cost.sql

with events as (
    select * from {{ ref('int_delayed_events') }}
)

select
    flight_id,
    flight_day,
    flight_status as status,
    delay_minutes,
    delay_cost_usd as delay_cost,
    supplier_id,
    route_id
from events
-- where flight_status = 'delayed'