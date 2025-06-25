with bookings as (
    select * from {{ ref('stg_passenger_bookings') }}
),

aggregated as (
  select
    flight_id,
    date_trunc('day', departure_time) as flight_day,
    count(*) as total_bookings,
    count(*) filter (where lower(booking_status) = 'cancelled') as cancelled_bookings,
    count(*) filter (where lower(booking_status) != 'cancelled') as confirmed_bookings,
    count(*) filter (where lower(booking_status) != 'cancelled') * 300 as estimated_revenue_usd,
    min(booking_time) as first_booking_time,
    max(booking_time) as last_booking_time
  from bookings
  group by flight_id, date_trunc('day', departure_time)
)

select * from aggregated