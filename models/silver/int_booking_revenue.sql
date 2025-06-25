with bookings as (
  select * from {{ ref('stg_passenger_bookings') }}
),

cleaned as (
  select
    booking_id,
    flight_id,
    flight_day,
    case 
      when booking_status = 'cancelled' then 0
      else 1
    end as is_confirmed,
    -- assume a flat $300 per booking
    case 
      when booking_status = 'cancelled' then 0
      else 300
    end as estimated_revenue,
    ingestion_date
  from bookings
)

select * from cleaned
