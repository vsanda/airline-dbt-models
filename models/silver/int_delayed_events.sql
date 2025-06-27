with flights as (
    select
        flight_id,
        flight_day,
        flight_status,
        delay_minutes,
        route_id
    from {{ ref('stg_flights') }}
),

crew_flags as (
    select
        flight_id,
        bool_or(crew_flagged) as crew_flag
    from {{ ref('stg_crew_payroll') }}
    where flight_id is not null
    group by flight_id
),

supplier_flags as (
    select
        flight_id,
        min(delivery_status) as delivery_status,
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
    f.route_id,
    s.supplier_id,
    s.delivery_status,
    c.crew_flag,

    -- Add delay_category
    case
        when c.crew_flag then 'crew'
        when s.delivery_status is not null then 'supplier'
        else null
    end as delay_category,

    case
        when c.crew_flag then 25           
        when s.delivery_status = 'delayed' then 15 
        else 10                           
    end as cost_per_minute


from flights f
left join supplier_flags s on f.flight_id = s.flight_id
left join crew_flags c on f.flight_id = c.flight_id
