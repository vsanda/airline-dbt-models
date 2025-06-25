with crew as (
    select * from {{ ref('stg_crew_payroll') }}
),

cleaned as (
    select
        crew_id,
        crew_name,
        '2025-07-08'::date as flight_day,  -- for testing purposes, replace with actual flight day if available
        crew_role,
        hours_logged,
        monthly_salary_usd,
        -- Assume 160 standard hours/month for cost per hour estimate
        case
            when hours_logged > 0 then round(monthly_salary_usd / 160.0, 2)
            else null
        end as estimated_hourly_rate,
        round((monthly_salary_usd / 160.0) * hours_logged, 2) as estimated_cost_allocated,
        ingestion_date
    from crew
)

select * from cleaned
