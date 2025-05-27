with purchase_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'purchase'
),

purchase_event_value AS (
    select
        _dlt_parent_id,
        max(if(key = 'tax', value__double_value, null)) as tax,
        max(if(key = 'transaction_id', coalesce(value__string_value, cast(value__int_value as string)), null)) as transaction_id,
        max(if(key = 'payment_type', value__string_value, null)) as payment_type,
        max(if(key = 'currency', value__string_value, null)) as currency,
        max(if(key = 'coupon', value__string_value, null)) as coupon,
        max(if(key = 'shipping_tier', value__string_value, null)) as shipping_tier
    from {{ source('ga4_full_sample', 'ga4_events__event_params') }}
    group by _dlt_parent_id
)

select
    event_row_id,
    purchase_completed_at,
    profile_id,
    session_id,
    purchase_first_seen_at,
    transaction_id,

    device_type,
    device_brand,
    device_model,
    os_name,
    os_version,
    browser_name,

    session_number,
    engagement_time_ms,
    purchase_landing_page_url,
    purchase_landing_page_title,
    purchase_page_referrer,


    payment_method,
    shipping_method,
    transaction_currency,
    applied_coupon,

    ecommerce__transaction_id,
    unique_items_count,
    total_item_quantity,
    purchase_revenue_usd,
    purchase_revenue,
    tax,
    tax_usd,

    user_ltv_revenue,
    user_ltv_currency

from (
    select
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.event_timestamp), second)
        ) as purchase_completed_at,
        bf.user_pseudo_id as profile_id,
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(bf.user_first_touch_timestamp), second)
        ) as purchase_first_seen_at,

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
        cast(bf.ecommerce__total_item_quantity as int64) as total_item_quantity,
        bf.ecommerce__purchase_revenue_in_usd as purchase_revenue_usd,
        bf.ecommerce__purchase_revenue as purchase_revenue,
        bf.ecommerce__tax_value_in_usd as tax_usd,
        bf._dlt_id as event_row_id,

        ev.ga_session_number as session_number,
        ev.page_location as purchase_landing_page_url,
        ev.page_title as purchase_landing_page_title,

        pv.tax,
        ev.engagement_time_msec as engagement_time_ms,
        pv.transaction_id,
        ev.ga_session_id as session_id,
        pv.payment_type as payment_method,
        pv.currency as transaction_currency,
        pv.coupon as applied_coupon,
        pv.shipping_tier as shipping_method,
        ev.page_referrer as purchase_page_referrer,

        row_number() over(
            partition by
                pv.transaction_id
            order by
                bf.event_timestamp
        ) as dedup_row

    from purchase_base_fields bf
    left join purchase_event_value pv
        on bf._dlt_id = pv._dlt_parent_id
    left join {{ ref('base_event_value') }} ev
    on bf._dlt_id = ev._dlt_parent_id
)
    where dedup_row = 1 and transaction_id is not null
