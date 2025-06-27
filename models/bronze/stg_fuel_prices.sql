with source as (
    select * from {{ source('raw_airline', 'crude_oil_prices') }}
),

cleaned as (
    select
        period,
        cast(period || '-01' as date) as price_month_date,
        duoarea,
        "area-name" as area_name,
        product,
        "product-name" as product_name,
        process,
        "process-name" as process_name,
        series,
        "series-description" as series_description,
        region,
        cast(price_month as date) as price_month,
        {{ dbt.safe_cast("price_per_gallon_usd", "float") }} as price_per_gallon_usd,
        fuel_category,
        cast(flight_day as date) as ingest_date
    from source
)

select * from cleaned
