with source as (
    select * from {{ source('raw_airline', 'payroll_data') }}
),

cleaned as (
    select
        crew_id,
        name as crew_name,
        lower(role) as crew_role,
        cast(hours_logged as int) as hours_logged,
        cast(monthly_salary_usd as numeric) as monthly_salary_usd,
        cast(recorded_at as date) as ingestion_date,
        flight_id,
        cast(duty_start as timestamp) as duty_start,
        cast(duty_end as timestamp) as duty_end,
        cast(hourly_rate_usd as numeric) as hourly_rate_usd,
        recorded_at
    from source
)

select * from cleaned
