with source as (
    select * from public.payment_stats
),

renamed as (
    select
        payment_type_desc,
        pickup_month,
        total_trips,
        avg_fare,
        avg_tip,
        total_revenue
    from source
)

select * from renamed