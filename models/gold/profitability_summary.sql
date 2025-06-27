with revenue as (

    select
        flight_id,
        total_revenue * 0.75 as revenue_total
    from {{ ref('revenue_summary') }}

),

costs as (

    select
        flight_id,
        crew_cost,
        fuel_cost,
        delay_cost,
        supplier_cost,  -- TODO: model supplier costs per flight if possible
        1500 as fixed_cost,  -- flat cost per flight
        crew_cost + fuel_cost + delay_cost + 15000 as total_cost
    from {{ ref('operational_cost') }}

),

joined as (

    select
        r.flight_id,
        r.revenue_total,
        c.fuel_cost,
        c.crew_cost,
        c.supplier_cost,
        c.delay_cost,
        c.fixed_cost,
        c.total_cost,

        -- gross profit: exclude fixed cost
        (r.revenue_total - (c.fuel_cost + c.crew_cost + c.delay_cost + c.supplier_cost)) as gross_profit,

        -- net profit: includes all costs
        (r.revenue_total - c.total_cost) as net_profit,

        -- profit margin %
        round((r.revenue_total - c.total_cost) * 100.0 / nullif(r.revenue_total, 0), 2) as profit_pct

    from revenue r
    left join costs c on r.flight_id = c.flight_id

)

select * from joined
