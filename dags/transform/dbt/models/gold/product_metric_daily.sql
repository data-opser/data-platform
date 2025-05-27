
with user_item_view_funnel as (
    select
        view_item_started_at,
        profile_id,
        session_id,
        product_id,
        product_name,
        max(view_item_count) as view_item_count,
        countif(add_card = 1) as add_to_cart,
        countif(purchase_completed = 1) as purchase,
        sum(if(purchase_completed = 1, purchase_revenue_usd, 0)) as purchase_revenue_usd
    from {{ ref('user_item_funnel') }}
    group by 1,2,3,4,5

)

select
    date_trunc(cast(view_item_started_at as timestamp), day) as as_of_day,
    product_id,
    product_name,
    count(*) as view_item_unique,
    sum(ifnull(view_item_count, 0)) as view_item_total,
    sum(ifnull(add_to_cart, 0)) as add_to_cart,
    sum(ifnull(purchase, 0)) as purchase,
    sum(ifnull(purchase_revenue_usd, 0)) as purchase_revenue_usd

from user_item_view_funnel
group by 1,2,3
