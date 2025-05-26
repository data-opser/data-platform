select
    vp.view_promo_started_at,
    ss.profile_id,
    ss.session_id,
    ss.session_first_seen_at,
    vp.view_promo_first_seen_at,
    vp.view_promo_time_session_ms,
    vp.view_promo_count,
    ss.session_number,

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

    ss.session_landing_page_title,
    ss.session_landing_page_url,
    ss.session_page_referrer,

    vp.view_promo_landing_page_title,
    vp.view_promo_landing_page_url,
    vp.view_promo_page_referrer,
    vp.promo_name,
    vp.promo_creative_name,

    if(sp.select_promo_started_at is not null, 1, 0) as select_promo_completed,
    sp.select_promo_started_at,
    sp.select_promo_first_seen_at,
    sp.select_promo_time_session_ms,
    sp.select_promo_count

from {{ ref('user_session_started') }} ss
inner join {{ ref('user_view_promotion') }} vp
    on
        ss.profile_id = vp.profile_id
        and ss.session_id = vp.session_id
left join {{ ref('user_select_promotion') }} sp
    on
        vp.profile_id = sp.profile_id
        and vp.session_id = sp.session_id
        and vp.promo_name = sp.promo_name