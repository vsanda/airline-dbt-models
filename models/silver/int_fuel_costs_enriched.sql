with fuel_raw as (
    select * from {{ ref('stg_fuel_prices') }}
),

enriched as (
    select
        -- Original fields
        product,
        product_name,
        process,
        process_name,
        region_name,
        price_per_gallon_usd,
        price_month,
        -- Convert to price per barrel if needed (1 barrel = 42 gallons)
        round(price_per_gallon_usd * 42, 2) as price_per_barrel,
        -- Categorize fuel type (could be expanded)
        case
            when lower(product_name) like '%diesel%' then 'diesel'
            when lower(product_name) like '%wti%' then 'jet_fuel'
            else 'other'
        end as fuel_category
    from fuel_raw
)

select * from enriched
