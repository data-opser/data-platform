
with promotion_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'view_promotion' --and event_date = '20201101'
{#    {% if is_incremental() -%}#}
{#        and parse_date('%Y%m%d', event_date) >= date_sub(current_date(), interval 2 day)#}
{#    {%- endif -%}#}
),

promotion_event_value AS (
    select
        _dlt_parent_id,
        max(if(key = 'engagement_time_msec', value__int_value, null)) as engagement_time_msec,
        max(if(key = 'ga_session_number', value__int_value, null)) as ga_session_number,
        max(if(key = 'page_title', value__string_value, null)) as page_title,
        max(if(key = 'engaged_session_event', value__int_value, null)) as engaged_session_event,
        max(if(key = 'session_engaged', value__string_value, null)) as session_engaged,
        max(if(key = 'ga_session_id', value__int_value, null)) as ga_session_id,
        max(if(key = 'page_location', value__string_value, null)) as page_location,
        max(if(key = 'page_referrer', value__string_value, null)) as page_referrer,
        max(if(key = 'campaign', value__string_value, null)) as campaign

    from {{ source('ga4_full_sample', 'ga4_events__event_params') }}
    group by _dlt_parent_id
),

promotion_items as (
    select
        _dlt_parent_id as promotion_event_id,
        item_id as campaign_code ,
        item_name as product_name,
        item_brand as product_brand,
        item_variant as product_variant,
        item_category as product_category,
        item_list_index as item_position_in_list,
        promotion_id,
        promotion_name,
        creative_name as promotion_creative_name
    from {{ source('ga4_full_sample', 'ga4_events__items') }}
)

select
    view_promotion_started_at,
    profile_id,
    session_id,
    first_seen_at,
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
    landing_page_title,
    landing_page_url,
    page_referrer,
    campaign,
    campaign_code,
    product_name,
    product_brand,
    product_variant,
    product_category,
    item_position_in_list,
    promotion_id,
    promotion_name,
    promotion_creative_name
from (
    select
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.event_timestamp), SECOND)) AS view_promotion_started_at,
        bf.user_pseudo_id as profile_id,
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.user_first_touch_timestamp), SECOND)) AS first_seen_at,

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

        ev.page_title as landing_page_title,
        ev.page_location as landing_page_url,
        ev.ga_session_id as session_id,
        ev.page_referrer,

        ev.campaign,

        pt.campaign_code,
        pt.product_name,
        pt.product_brand,
        pt.product_variant,
        pt.product_category,
        pt.item_position_in_list,
        pt.promotion_id,
        pt.promotion_name,
        pt.promotion_creative_name,

        row_number() over(
            partition by
                bf.user_pseudo_id, ev.ga_session_id
            order by
                bf.event_timestamp
        ) as dedup_row

    from promotion_base_fields bf
    left join promotion_event_value ev
        on bf._dlt_id = ev._dlt_parent_id
    left join promotion_items pt
        on bf._dlt_id = pt.promotion_event_id
)
    where dedup_row = 1
