with crew as (
  select 
    flight_day, 
    sum(monthly_salary_usd) as total_crew_cost
  from {{ ref('int_crew_costs') }}
    group by flight_day
),

fuel as (
  select 
    '2025-07-08'::date as flight_day, -- for testing purposes, replace with actual flight day if available
    avg(price_per_gallon_usd) * 500 as estimated_fuel_cost  -- assume 500 gallons per flight
  from {{ ref('int_fuel_costs_enriched') }}
    group by flight_day
),

aircraft as (
  select 
    flight_day,  
    aircraft_status
  from {{ ref('int_plane_inventory_enriched') }}
  limit 1  -- just take one for now
)

select
  crew.flight_day,
  crew.total_crew_cost,
  fuel.estimated_fuel_cost,
  aircraft.aircraft_status,
  (crew.total_crew_cost + coalesce(fuel.estimated_fuel_cost, 0)) as total_operational_cost
from crew
left join fuel using (flight_day)
left join aircraft using (flight_day)
