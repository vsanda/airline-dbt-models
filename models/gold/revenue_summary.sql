-- models/gold/fct_revenue_summary.sql

with base_revenue as (

    select
        r.flight_id,
        r.total_revenue,
        r.base_fare,
        r.baggage_fees,
        r.upgrade_revenue,
        r.ancillary,
        r.booking_channel,
        r.customer_segment,
        r.passenger_count
    from {{ ref('int_revenue_metrics') }} r  -- your silver-layer cleaned revenue input
),

aggregated as (

    select
        flight_id,
        sum(base_fare) as base_fare,
        sum(baggage_fees) as baggage_fees,
        sum(upgrade_revenue) as upgrade_revenue,
        sum(ancillary) as ancillary,
        sum(passenger_count) as passenger_count,
        sum(base_fare + baggage_fees + upgrade_revenue + ancillary) as total_revenue
    from base_revenue
    group by flight_id

),

final as (

    select
        flight_id,
        total_revenue,
        base_fare,
        baggage_fees,
        upgrade_revenue,
        ancillary,
        passenger_count,
        round(total_revenue / nullif(passenger_count, 0), 2) as avg_revenue_per_passenger
    from aggregated

)

select * from final