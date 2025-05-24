
select
    ss.session_started_at,
    ss.profile_id,
    ss.session_id,
    ss.first_seen_at,
    ss.session_number,
    ss.session_engagement_time_ms,

    ss.device_type,
    ss.device_brand,
    ss.device_model,
    ss.os_name,
    ss.os_version,
    ss.browser_name,

    ss.geo_continent,
    ss.geo_subcontinent,
    ss.geo_country,
    ss.geo_region,
    ss.geo_city,

    ss.platform,
    ss.traffic_source_name,
    ss.traffic_source_origin,

    ss.landing_page_title,
    ss.landing_page_url,

    tc.transaction_id,
    tc.payment_method,
    tc.shipping_method,
    tc.transaction_currency,
    tc.applied_coupon,

    tc.unique_items_count,
    tc.total_item_quantity,

    tc.purchase_revenue_usd,
    tc.tax_usd,
    tc.user_ltv_revenue,
    tc.user_ltv_currency,

    if(ss.session_number = 1, 1, 0) AS onboarding_started,
    if(cs.checkout_started_at is not null, 1, 0) as checkout_presented,
    if(tc.purchase_completed_at is not null, 1, 0) as purchase_completed,
    if(vp.view_promotion_started_at is not null, 1, 0) as view_promotion_completed

from {{ ref('user_session_started') }} ss
left join {{ ref('user_transaction_completed')}} tc
    on
        ss.profile_id = tc.profile_id
        and ss.session_id = tc.session_id
left join {{ ref('user_checkout_screen') }} cs
    on
        ss.profile_id = cs.profile_id
        and ss.session_id = cs.session_id
left join {{ ref('user_view_promotion') }} vp
    on
        ss.profile_id = vp.profile_id
        and ss.session_id = vp.session_id
