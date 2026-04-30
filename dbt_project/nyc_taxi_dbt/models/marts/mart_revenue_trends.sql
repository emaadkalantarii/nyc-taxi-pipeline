with daily as (
    select * from {{ ref('stg_daily_summary') }}
),

revenue_trends as (
    select
        trip_date,
        pickup_month,
        total_trips,
        daily_revenue,
        avg_fare,
        avg_distance,
        active_pickup_zones,
        round(
            avg(daily_revenue) over (
                order by trip_date
                rows between 6 preceding and current row
            )::numeric, 2
        )                                           as revenue_7day_avg,
        round(
            avg(total_trips) over (
                order by trip_date
                rows between 6 preceding and current row
            )::numeric, 0
        )                                           as trips_7day_avg,
        round(
            (daily_revenue - lag(daily_revenue, 1) over (order by trip_date))
            / nullif(lag(daily_revenue, 1) over (order by trip_date), 0)
            * 100
        )::numeric                                  as revenue_day_over_day_pct
    from daily
    where trip_date >= '2024-01-01'
    order by trip_date
)

select * from revenue_trends