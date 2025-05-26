with purchase_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'purchase' --and event_date = '20201101'
{#    {% if is_incremental() -%}#}
{#        and parse_date('%Y%m%d', event_date) >= date_sub(current_date(), interval 2 day)#}
{#    {%- endif -%}#}
),


purchase_event_value AS (
    select
        _dlt_parent_id,
        max(if(key = 'ga_session_number', value__int_value, null)) as ga_session_number,
        max(if(key = 'value', value__double_value, null)) as value,
        max(if(key = 'session_engaged', value__string_value, null)) as session_engaged,
        max(if(key = 'page_location', value__string_value, null)) as page_location,
        max(if(key = 'page_title', value__string_value, null)) as page_title,
        max(if(key = 'engaged_session_event', value__int_value, null)) as engaged_session_event,
        max(if(key = 'tax', value__double_value, null)) as tax,
        max(if(key = 'engagement_time_msec', value__int_value, null)) as engagement_time_msec,
        max(if(key = 'transaction_id', coalesce(value__string_value, cast(value__int_value as string)), null)) as transaction_id,
        max(if(key = 'ga_session_id', value__int_value, null)) as ga_session_id,
        max(if(key = 'payment_type', value__string_value, null)) as payment_type,
        max(if(key = 'currency', value__string_value, null)) as currency,
        max(if(key = 'coupon', value__string_value, null)) as coupon,
        max(if(key = 'campaign', value__string_value, null)) as campaign,
        max(if(key = 'source', value__string_value, null)) as source,
        max(if(key = 'shipping_tier', value__string_value, null)) as shipping_tier,
        max(if(key = 'promotion_name', value__string_value, null)) as promotion_name,
        max(if(key = 'page_referrer', value__string_value, null)) as page_referrer
    from {{ source('ga4_full_sample', 'ga4_events__event_params') }}
    group by _dlt_parent_id
)

select
    event_row_id,
    purchase_completed_at,
    profile_id,
    session_id,
    first_seen_at,
    transaction_id,

    device_type,
    device_brand,
    device_model,
    os_name,
    os_version,
    browser_name,

    session_number,
    engagement_time_ms,
    value,
    landing_page_url,
    landing_page_title,
    marketing_campaign,
    promotion_name,
    page_referrer,
    traffic_source,

    payment_method,
    shipping_method,
    transaction_currency,
    applied_coupon,

    ecommerce__transaction_id,
    unique_items_count,
    event_value_in_usd,
    total_item_quantity,
    purchase_revenue_usd,
    purchase_revenue,
    tax,
    tax_usd,

    user_ltv_revenue,
    user_ltv_currency

from (
    select
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.event_timestamp), SECOND)) as purchase_completed_at,
        bf.user_pseudo_id as profile_id,
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.user_first_touch_timestamp), SECOND)) as first_seen_at,

        bf.user_ltv__revenue as user_ltv_revenue,
        bf.user_ltv__currency as user_ltv_currency,

        bf.device__category as device_type,
        bf.device__mobile_brand_name as device_brand,
        bf.device__mobile_model_name as device_model,
        bf.device__operating_system as os_name,
        bf.device__operating_system_version as os_version,
        bf.device__web_info__browser as browser_name,

        bf.ecommerce__transaction_id,
        cast(bf.ecommerce__unique_items as int64) as unique_items_count,
        bf.event_value_in_usd,
        cast(bf.ecommerce__total_item_quantity as int64) as total_item_quantity,
        bf.ecommerce__purchase_revenue_in_usd as purchase_revenue_usd,
        bf.ecommerce__purchase_revenue as purchase_revenue,
        bf.ecommerce__tax_value_in_usd as tax_usd,
        bf._dlt_id as event_row_id,

        ev.ga_session_number as session_number,
        ev.value,
        ev.page_location as landing_page_url,
        ev.page_title as landing_page_title,

        ev.tax,
        ev.engagement_time_msec as engagement_time_ms,
        ev.transaction_id,
        ev.ga_session_id as session_id,
        ev.payment_type as payment_method,
        ev.currency as transaction_currency,
        ev.coupon as applied_coupon,
        ev.campaign as marketing_campaign,
        ev.source as traffic_source,
        ev.shipping_tier as shipping_method,
        ev.promotion_name,
        ev.page_referrer,

        row_number() over(
            partition by
                ev.transaction_id
            order by
                bf.event_timestamp
        ) as dedup_row

    from purchase_base_fields bf
    left join purchase_event_value ev
        on bf._dlt_id = ev._dlt_parent_id
)
    where dedup_row = 1
