-- models/gold/fct_operational_cost_summary.sql

with crew_costs as (

    select
        f.flight_id,
        f.route_id,
        null as supplier_id,
        'crew' as cost_category,
        round(f.crew_cost_total, 2) as cost_usd
    from {{ ref('crew_summary') }} f
    where f.crew_cost_total is not null

),

fuel_costs as (

    select
        f.flight_id,
        null as route_id,
        null as supplier_id,
        'fuel' as cost_category,
        round(f.fuel_cost, 2) as cost_usd
    from {{ ref('fuel_summary') }} f
    where f.fuel_cost is not null

),

delay_costs as (

    select
        d.flight_id,
        null as route_id,
        null as supplier_id,
        'delay' as cost_category,
        round(d.delay_cost, 2) as cost_usd
    from {{ ref('delay_summary') }} d
    where d.delay_cost is not null

),

supplier_costs as (

    select
        flight_id,
        null as route_id,
        s.supplier_id,
        'supplier' as cost_category,
        round(sum(cost_usd)::numeric,2) as cost_usd
    from {{ ref('int_supplier_metrics') }} s
    where s.cost_usd is not null
    group by 1, 2, 3, 4

),

all_costs as (

    select * from crew_costs
    union all
    select * from fuel_costs
    union all
    select * from delay_costs
    union all
    select * from supplier_costs

)

, cost_pivot as (

    select
        flight_id,
        sum(case when cost_category = 'crew' then cost_usd else 0 end) as crew_cost,
        sum(case when cost_category = 'fuel' then cost_usd else 0 end) as fuel_cost,
        sum(case when cost_category = 'delay' then cost_usd else 0 end) as delay_cost,
        sum(case when cost_category = 'supplier' then cost_usd else 0 end) as supplier_cost,
        round(sum(cost_usd::numeric),2) as total_operational_cost
    from all_costs
    where flight_id is not null
    group by flight_id

)

select * from cost_pivot
