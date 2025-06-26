
with flights as (
    select * from {{ ref('stg_flights') }}
),
crew as (
    select 
        flight_id,
        sum(monthly_salary_usd) as crew_cost_total,
        count(*) as num_crew,
        sum(hours_logged) as total_crew_hours
    from {{ ref('stg_crew_payroll') }}
    group by flight_id
),
suppliers as (
    select 
        flight_id,
        sum(part_cost_usd) as supplier_cost_total
    from {{ ref('stg_supplier_logs') }}
    where flight_id is not null
    group by flight_id
),
metrics as (
    select 
        f.flight_id,
        f.flight_day,
        f.aircraft_id,
        f.route_id,
        f.flight_status,
        f.delay_minutes,
        f.delay_cost_usd,

        coalesce(c.crew_cost_total, 0) as crew_cost_usd,
        coalesce(s.supplier_cost_total, 0) as supplier_cost_usd,
        coalesce(c.crew_cost_total, 0) + coalesce(s.supplier_cost_total, 0) as total_cost_usd

    from flights f
    left join crew c on f.flight_id = c.flight_id
    left join suppliers s on f.flight_id = s.flight_id
)

select * from metrics
