with supplier_stats as (

    select
        supplier_id,
        service_type as category,
        sum(cost_usd) as total_cost,
        sum(delay_flag) as delay_count,
        avg(cost_usd) as avg_unit_cost,

        case 
            when sum(delay_flag) > 5 or avg(cost_usd) > 10000 then 'investigate'
            else 'fine'
        end as status_flag

    from {{ ref('int_supplier_metrics') }}
    group by supplier_id, service_type

)

select * from supplier_stats
