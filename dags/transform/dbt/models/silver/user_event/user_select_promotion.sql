
with promotion_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'select_promotion'
),

promotion_items as (
    select
        _dlt_parent_id as promo_event_id,
        item_list_index as item_position_in_list,
        promotion_name as promo_name,
        creative_name as promo_creative_name
    from {{ source('ga4_full_sample', 'ga4_events__items') }}
)

select
    select_promo_started_at,
    profile_id,
    session_id,
    select_promo_first_seen_at,
    select_promo_time_session_ms,
    select_promo_count,
    device_type,
    device_brand,
    device_model,
    os_name,
    os_version,
    browser_name,
    traffic_source_medium,
    traffic_source_name,
    traffic_source_origin,
    platform,
    select_promo_landing_page_title,
    select_promo_landing_page_url,
    select_promo_page_referrer,
    item_position_in_list,
    promo_name,
    promo_creative_name
from (
    select
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.event_timestamp), second)
        ) AS select_promo_started_at,
        bf.user_pseudo_id as profile_id,
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.user_first_touch_timestamp), second)
        ) AS select_promo_first_seen_at,

        bf.device__category as device_type,
        bf.device__mobile_brand_name as device_brand,
        bf.device__mobile_model_name as device_model,
        bf.device__operating_system as os_name,
        bf.device__operating_system_version as os_version,
        bf.device__web_info__browser as browser_name,

        bf.traffic_source__medium traffic_source_medium,
        bf.traffic_source__name as traffic_source_name,
        bf.traffic_source__source as traffic_source_origin,
        bf.platform,

        ev.engagement_time_msec as select_promo_time_session_ms,
        ev.page_title as select_promo_landing_page_title,
        ev.page_location as select_promo_landing_page_url,
        ev.ga_session_id as session_id,
        ev.page_referrer as select_promo_page_referrer,

        pt.item_position_in_list,
        pt.promo_name,
        pt.promo_creative_name,

        count(distinct event_timestamp) over (
            partition by bf.user_pseudo_id, ev.ga_session_id, pt.promo_name
        ) as select_promo_count,

        row_number() over(
            partition by
                bf.user_pseudo_id, ev.ga_session_id, pt.promo_name
            order by
                bf.event_timestamp
        ) as dedup_row

    from promotion_base_fields bf
    left join {{ ref('base_event_value') }} ev
        on bf._dlt_id = ev._dlt_parent_id
    left join promotion_items pt
        on bf._dlt_id = pt.promo_event_id
)
    where dedup_row = 1 and promo_name is not null
