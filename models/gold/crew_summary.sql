with role_counts as (

    select
        flight_id,
        sum(case when lower(crew_role) in ('captain', 'first officer') then 1 else 0 end) as num_pilots,
        sum(case when lower(crew_role) = 'flight attendant' then 1 else 0 end) as num_attendants
    from {{ ref('stg_crew_payroll') }}  -- or 'stg_crew_logs' if it's cleaned
    group by flight_id

),

crew_summary as (

    select
        ca.flight_id,
        count(*) as num_crew,
        avg_rate,
        rc.num_pilots,
        rc.num_attendants,
        concat(rc.num_pilots, 'P / ', rc.num_attendants, 'A') as role_mix,
        bool_or(ca.crew_flag) as crew_flag  -- optional, assumes a flagged column
    from {{ ref('int_crew_metrics') }} ca
    left join role_counts rc on ca.flight_id = rc.flight_id
    group by ca.flight_id, rc.num_pilots, rc.num_attendants, avg_rate

),

base as (

    select
        f.flight_id,
        f.route_id,
        f.aircraft_type,
        c.num_crew,
        c.avg_rate * 10,
        u.flight_hours,
        round(c.num_crew * c.avg_rate * 10 * u.flight_hours, 2) as crew_cost_total,
        c.role_mix,
        c.crew_flag
    from {{ ref('int_flights_metrics') }} f
    left join crew_summary c
        on f.flight_id = c.flight_id
    left join {{ ref('int_fuel_metrics') }} u
        on f.flight_id = u.flight_id

)

select * from base
