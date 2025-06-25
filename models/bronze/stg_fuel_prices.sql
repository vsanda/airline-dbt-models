with source as (
    select * from {{ source('raw_airline', 'crude_oil_prices') }}
),

cleaned as (
    select
        to_date(period || '-01', 'YYYY-MM-DD') as price_month,
        duoarea as region_code,
        trim("area-name") as region_name,
        product,
        trim("product-name") as product_name,
        process,
        trim("process-name") as process_name,
        series,
        trim("series-description") as series_description,
        {{ dbt.safe_cast("price_per_gallon", "int") }} as price_per_gallon_usd
    from source
)

select * from cleaned
