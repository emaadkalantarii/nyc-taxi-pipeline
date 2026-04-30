with source as (
    select * from public.daily_summary
),

renamed as (
    select
        trip_date,
        pickup_month,
        total_trips,
        daily_revenue,
        avg_distance,
        avg_fare,
        active_pickup_zones
    from source
)

select * from renamed