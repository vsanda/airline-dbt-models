with source as (
    select * from {{ source('raw_airline', 'passenger_data') }}
),

cleaned as (
    select
        booking_id,
        passenger_name,
        flight_id,
        aircraft_id,
        seat_number,
        cast(departure_time as timestamp) as departure_time,
        cast(booking_time as timestamp) as booking_time,
        lower(booking_status) as booking_status,
        {{ dbt.safe_cast("ticket_price", "numeric") }} as ticket_price,
        {{ dbt.safe_cast("baggage_fee", "numeric") }} as baggage_fee,
        {{ dbt.safe_cast("upgrades", "numeric") }} as upgrade_fee,
        lower(channel) as booking_channel,
        lower(customer_segment) as customer_segment,
        lower(aircraft_type) as aircraft_type,
        cast(flight_day as date) as flight_day,
        currency,
        cast(recorded_at as timestamp) as recorded_at
    from source
)


select * from cleaned
