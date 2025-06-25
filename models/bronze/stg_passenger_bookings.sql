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
        lower(status) as booking_status,
        {{ dbt.safe_cast("date", "date") }} as ingestion_date
    from source
)

select * from cleaned
