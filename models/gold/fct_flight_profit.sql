
with rev as (
  select * from {{ ref('fct_revenue_summary') }}
),
costs as (
  select * from {{ ref('fct_operational_costs') }}
)

select
  rev.flight_day,
  rev.flight_count,
  rev.total_revenue_usd,
  costs.total_operational_cost,
  rev.total_revenue_usd - costs.total_operational_cost as profit_usd
from rev
left join costs using (flight_day)
