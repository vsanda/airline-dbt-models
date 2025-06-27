with supplier_logs as (

    select
        flight_id,
        supplier_id,
        service_type,
        flight_day,
        sum(cost_usd) as logged_cost,
        max(case when lower(delivery_status) = 'delayed' then 1 else 0 end) as delay_flag
    from {{ ref('stg_supplier_logs') }}
    group by flight_id, supplier_id, service_type, flight_day

),

fuel_costs as (

    select
        price_month_date,
        min(fuel_cost) as fuel_cost_usd
    from {{ ref('int_fuel_metrics') }}
    group by price_month_date

),

combined as (

    select
        s.flight_id,
        s.supplier_id,
        s.service_type,

        case 
            when lower(s.service_type) = 'fuel' and f.fuel_cost_usd is not null 
                then f.fuel_cost_usd
            else s.logged_cost
        end as cost_usd,

        s.delay_flag

    from supplier_logs s
    left join fuel_costs f
        on date_trunc('month', s.flight_day) = date_trunc('month', f.price_month_date)

)

select * from combined
