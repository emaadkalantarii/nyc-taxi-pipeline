with source as (
    select * from public.hourly_stats
),

renamed as (
    select
        pickup_month,
        pickup_day_of_week,
        pickup_hour,
        time_of_day,
        is_weekend,
        total_trips,
        avg_distance_miles,
        avg_duration_minutes,
        avg_fare,
        total_revenue,
        avg_tip_pct,
        avg_speed_mph
    from source
)

select * from renamed