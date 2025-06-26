-- silver_crew_metrics.sql

with base as (
    select
        flight_id,
        count(*) as num_crew,
        sum(hours_logged) as total_crew_hours,
        avg(hourly_rate_usd) as avg_hourly_rate_usd,
        sum(hours_logged) * avg(hourly_rate_usd) as crew_cost_total_usd
    from {{ ref('stg_crew_payroll') }}
    where flight_id is not null
    group by flight_id
)

select * from base
