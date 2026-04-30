with source as (
    select * from public.location_stats
),

renamed as (
    select
        pu_location_id,
        total_pickups,
        avg_fare,
        avg_distance,
        avg_tip_pct,
        total_revenue
    from source
)

select * from renamed