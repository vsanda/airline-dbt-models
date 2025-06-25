with planes as (
    select * from {{ ref('stg_plane_inventory') }}
),

enriched as (
    select
        aircraft_id,
        aircraft_model,
        manufacturer,
        seat_capacity,
        aircraft_status,
        '2025-07-08'::date as flight_day,  -- for testing purposes, replace with actual flight day if available
        case
            when lower(trim(aircraft_status)) = 'retired' then false
            when lower(trim(aircraft_status)) = 'maintenance' then false
            else true
        end as is_operational,
        cast(ingestion_date as date) as snapshot_date
    from planes
)

select * from enriched
