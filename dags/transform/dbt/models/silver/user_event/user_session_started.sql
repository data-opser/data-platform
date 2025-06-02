
with session_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'session_start'
)

select
    session_started_at,
    profile_id,
    session_id,
    session_first_seen_at,
    session_number,

    device_type,
    device_brand,
    device_model,
    os_name,
    os_version,
    browser_name,

    geo_continent,
    geo_subcontinent,
    geo_country,
    geo_region,
    geo_city,

    platform,
    traffic_source_name,
    traffic_source_origin,

    session_landing_page_title,
    session_landing_page_url,
    session_page_referrer

from (
    select
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.event_timestamp), second)
        ) as session_started_at,
        bf.user_pseudo_id as profile_id,
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.user_first_touch_timestamp), second)
        ) as session_first_seen_at,

        bf.device__category as device_type,
        bf.device__mobile_brand_name as device_brand,
        bf.device__mobile_model_name as device_model,
        bf.device__operating_system as os_name,
        bf.device__operating_system_version as os_version,
        bf.device__web_info__browser as browser_name,

        bf.geo__continent as geo_continent,
        bf.geo__sub_continent as geo_subcontinent,
        bf.geo__country as geo_country,
        bf.geo__region as geo_region,
        bf.geo__city as geo_city,

        bf.platform,
        bf.traffic_source__name as traffic_source_name,
        bf.traffic_source__source as traffic_source_origin,

        ev.page_title as session_landing_page_title,
        ev.page_location as session_landing_page_url,
        ev.page_referrer as session_page_referrer,

        ev.ga_session_id as session_id,
        ev.ga_session_number as session_number,

        row_number() over(
            partition by
                bf.user_pseudo_id, ev.ga_session_id
            order by
                bf.event_timestamp
        ) as dedup_row

    from session_base_fields bf
    left join {{ ref('base_event_value') }} ev
        on bf._dlt_id = ev._dlt_parent_id
)
    where dedup_row = 1
