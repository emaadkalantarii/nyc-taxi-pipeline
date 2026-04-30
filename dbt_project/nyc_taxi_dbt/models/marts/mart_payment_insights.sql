with payments as (
    select * from {{ ref('stg_payment_stats') }}
),

insights as (
    select
        payment_type_desc,
        pickup_month,
        total_trips,
        avg_fare,
        avg_tip,
        total_revenue,
        round(
            (avg_tip / nullif(avg_fare, 0) * 100)::numeric, 2
        )                                           as tip_rate_pct,
        round(
            (total_trips * 100.0 / sum(total_trips) over (
                partition by pickup_month
            ))::numeric, 2
        )                                           as market_share_pct
    from payments
)

select * from insights
order by pickup_month, total_trips desc