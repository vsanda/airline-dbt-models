with source as (
    select * from {{ source('raw_airline', 'payroll_data') }}
),

cleaned as (
    select
        crew_id,
        name as crew_name,
        lower(role) as crew_role,
        cast(hours_logged as int) as hours_logged,
        cast(monthly_salary as numeric) as monthly_salary_usd,
        cast(date as date) as ingestion_date
    from source
)

select * from cleaned
