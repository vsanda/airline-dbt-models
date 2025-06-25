with revenue as (
  select * from {{ ref('int_booking_metrics') }}
)

select
  flight_day,
  sum(estimated_revenue_usd) as total_revenue_usd,
  sum(confirmed_bookings) as confirmed_bookings,
  sum(cancelled_bookings) as cancelled_bookings,
  count(*) as flight_count
from revenue
group by 1

