with locations as (
    select * from {{ ref('stg_location_stats') }}
),

performance as (
    select
        pu_location_id,
        total_pickups,
        avg_fare,
        avg_distance,
        avg_tip_pct,
        total_revenue,
        round(
            (total_revenue / nullif(total_pickups, 0))::numeric, 2
        )                                           as revenue_per_trip,
        ntile(4) over (
            order by total_pickups
        )                                           as pickup_volume_quartile,
        ntile(4) over (
            order by total_revenue
        )                                           as revenue_quartile,
        case
            when ntile(4) over (order by total_revenue) = 4 then 'High Value'
            when ntile(4) over (order by total_revenue) = 3 then 'Medium Value'
            when ntile(4) over (order by total_revenue) = 2 then 'Low Value'
            else 'Minimal Value'
        end                                         as location_tier
    from locations
)

select * from performance
order by total_revenue desc