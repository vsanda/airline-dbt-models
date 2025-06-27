with base as (

    select
        d.flight_id,
        d.flight_status,
        d.delay_minutes,
        d.delay_category,
        d.supplier_id,
        d.delay_minutes * coalesce(d.cost_per_minute, 75) as delay_cost,  
        d.cost_per_minute
        -- d.recorded_at

    from {{ ref('int_delayed_events') }} d

)

select * from base
