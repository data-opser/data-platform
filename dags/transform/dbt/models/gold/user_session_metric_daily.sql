
with user_session as (
    select
        profile_id,
        session_id,
        max(session_started_at) as session_started_at,
        max(geo_continent) as geo_continent,
        max(geo_country) as geo_country,

        max(device_type) as device_type,
        max(device_brand) as device_brand,
        max(browser_name) as browser_name,

        max(onboarding_started) as onboarding_started,
        max(checkout_presented) as checkout_presented,

        max(view_promo_completed) as view_promo_completed,
        max(select_promo_completed) as select_promo_completed,

        max(view_promo_unique) as view_promo_unique,
        max(view_promo_total) as view_promo_total,

        max(select_promo_unique) as select_promo_unique,
        max(select_promo_total) as select_promo_total,

        sum(purchase_completed) as purchase_completed,
        sum(purchase_revenue_usd) as purchase_revenue_usd,

        sum(unique_items_count) as unique_items_count,
        sum(total_item_quantity) as total_item_quantity

    from {{ ref('user_purchase_transaction_session') }}
    group by 1, 2

)


select
    date_trunc(cast(session_started_at as timestamp), day) as as_of_day,
    geo_continent,
    geo_country,
    device_type,
    device_brand,
    browser_name,

    count(session_started_at) as session_started,

    sum(ifnull(onboarding_started, 0)) as onboarding_started,
    sum(ifnull(checkout_presented, 0)) as checkout_presented,

    sum(ifnull(view_promo_completed, 0)) as view_promo_completed,
    sum(ifnull(select_promo_completed, 0)) as select_promo_completed,

    sum(ifnull(view_promo_unique, 0)) as view_promo_unique,
    sum(ifnull(view_promo_total, 0)) as view_promo_total,

    sum(ifnull(select_promo_unique, 0)) as select_promo_unique,
    sum(ifnull(select_promo_total, 0)) as select_promo_total,

    sum(ifnull(purchase_completed, 0)) as purchase_completed,

    sum(ifnull(purchase_revenue_usd, 0)) as purchase_revenue_usd,

    sum(ifnull(unique_items_count, 0)) as unique_items_count,
    sum(ifnull(total_item_quantity, 0)) as total_item_quantity

from user_session
group by 1, 2, 3, 4, 5, 6
