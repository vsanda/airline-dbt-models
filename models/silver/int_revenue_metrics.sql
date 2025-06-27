-- models/intermediate/int_revenue_metrics.sql

with bookings as (
    select *
    from {{ ref('stg_bookings') }}
),

aggregated as (
    select
        flight_id,
        booking_channel,
        customer_segment,
        count(booking_id) as passenger_count,
        round(sum(ticket_price), 2) as base_fare,
        round(sum(baggage_fee), 2) as baggage_fees,
        round(sum(upgrade_fee), 2) as upgrade_revenue,
        round(sum(ticket_price + baggage_fee + upgrade_fee), 2) as total_revenue,
        round(avg(ticket_price + baggage_fee + upgrade_fee), 2) as avg_rev_per_passenger
    from bookings
    where booking_status != 'cancelled'
    group by flight_id, booking_channel, customer_segment
)

select
    *,
    round(total_revenue * 0.05, 2) as ancillary  
from aggregated
