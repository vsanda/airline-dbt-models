with source as (
    select * from {{ source('raw_airline', 'passenger_data') }}
),

cleaned as (
    select
        booking_id,
        passenger_name,
        flight_id,
        upper(seat_number) as seat_number,
        cast(departure_time as date) as flight_day,
        {{ dbt.safe_cast("departure_time", "timestamp") }} as departure_time,
        {{ dbt.safe_cast("booking_time", "timestamp") }} as booking_time,
        lower(booking_status) as booking_status,
        {{ dbt.safe_cast("ticket_price", "numeric") }} as ticket_price_usd,
        {{ dbt.safe_cast("baggage_fee", "numeric") }} as baggage_fee_usd,
        {{ dbt.safe_cast("upgrades", "numeric") }} as upgrades_usd,
        lower(channel) as booking_channel,
        lower(customer_segment) as customer_segment,
        currency,
        {{ dbt.safe_cast("recorded_at", "timestamp") }} as ingestion_timestamp,
        cast(recorded_at as date) as ingestion_date
    from source
)

select * from cleaned
