with hourly as (
    select * from {{ ref('stg_hourly_stats') }}
),

peak_analysis as (
    select
        pickup_hour,
        time_of_day,
        is_weekend,
        sum(total_trips)                            as total_trips,
        round(avg(avg_fare)::numeric, 2)            as avg_fare,
        round(avg(avg_speed_mph)::numeric, 2)       as avg_speed_mph,
        round(avg(avg_tip_pct)::numeric, 2)         as avg_tip_pct,
        round(sum(total_revenue)::numeric, 2)       as total_revenue,
        round(avg(avg_duration_minutes)::numeric, 2) as avg_duration_minutes,
        case
            when sum(total_trips) > 300000 then 'Peak'
            when sum(total_trips) > 150000 then 'Moderate'
            else 'Off-Peak'
        end                                         as demand_level
    from hourly
    group by pickup_hour, time_of_day, is_weekend
    order by total_trips desc
)

select * from peak_analysis