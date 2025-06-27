with source as (
    select * from {{ source('raw_airline', 'payroll_data') }}
),

cleaned as (
    select
        crew_id,
        name as crew_name,
        lower(role) as crew_role,
        base_location,
        cast(hours_logged as int) as hours_logged,
        cast(hourly_rate_usd as numeric) as hourly_rate_usd,
        cast(crew_flagged as boolean) as crew_flagged,
        cast(duty_start as timestamp) as duty_start,
        cast(duty_end as timestamp) as duty_end,
        cast(recorded_at as timestamp) as recorded_at,
        cast(recorded_at as date) as ingestion_date,
        flight_id,
        flight_day
    from source
)


select * from cleaned
