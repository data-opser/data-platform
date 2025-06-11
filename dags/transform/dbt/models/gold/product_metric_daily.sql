
with user_item_view_funnel as (
    select
        profile_id,
        session_id,
        product_id,
        product_name,

        max(view_item_started_at) as view_item_started_at,
        max(view_item_count) as total_view_item_count,
        max(geo_continent) as geo_continent,
        max(geo_country) as geo_country,

        countif(add_card = 1) as add_to_cart,
        countif(item_purchase_completed = 1) as item_purchase_completed,
{#        sum(if(purchase_completed = 1, purchase_revenue_usd, 0)) as purchase_revenue_usd#}
        sum(product_revenue_usd) as product_revenue_usd

    from {{ ref('user_item_funnel') }}
    group by 1,2,3,4

)

select
    date_trunc(cast(view_item_started_at as timestamp), day) as as_of_day,
    product_id,
    product_name,
    geo_continent,
    geo_country,

    count(*) as view_item_unique,
    sum(total_view_item_count) as total_view_item_count,
    sum(add_to_cart) as add_to_cart,
    sum(item_purchase_completed) as item_purchase_completed,
    sum(product_revenue_usd) as product_revenue_usd

from user_item_view_funnel
group by 1, 2, 3, 4, 5
