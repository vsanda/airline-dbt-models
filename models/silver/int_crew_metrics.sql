-- models/intermediate/int_crew_assignments.sql

with crew as (
    select *
    from {{ ref('stg_crew_payroll') }}
),

flights as (
    select
        flight_id,
        flight_day,
        departure_time,
        arrival_time,
        extract(epoch from (arrival_time - departure_time)) / 3600.0 as flight_hours
    from {{ ref('int_flights_metrics') }}
),

joined as (
    select
        c.flight_id,
        f.flight_hours,
        c.crew_id,
        c.hourly_rate_usd,
        c.crew_flagged
    from crew c
    left join flights f
      on c.flight_id = f.flight_id
),

aggregated as (
    select
        flight_id,
        count(*) as num_crew,
        round(avg(hourly_rate_usd), 2) as avg_rate,
        max(flight_hours) as flight_hours,
        round(count(*) * avg(hourly_rate_usd) * max(flight_hours), 2) as crew_cost,
        bool_or(crew_flagged) as crew_flag
    from joined
    group by flight_id
)

select * from aggregated
