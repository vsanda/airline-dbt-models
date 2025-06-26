-- silver_flight_revenue.sql

with bookings as (
    select
        flight_id,
        lower(booking_channel) as channel,
        lower(customer_segment) as segment,
        sum(ticket_price_usd) as base_fare_usd,
        sum(baggage_fee_usd) as baggage_fees_usd,
        sum(upgrades_usd) as upgrades_usd,
        sum(ticket_price_usd) + sum(baggage_fee_usd) + sum(upgrades_usd) as total_revenue_usd,
        count(*) as num_passengers
    from {{ ref('stg_passenger_bookings') }}
    where lower(booking_status) = 'confirmed'
    group by flight_id, lower(booking_channel), lower(customer_segment)
)

select
    flight_id,
    channel,
    segment,
    base_fare_usd,
    baggage_fees_usd,
    upgrades_usd,
    total_revenue_usd,
    num_passengers
from bookings
