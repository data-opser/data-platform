with add_card_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'add_to_cart'
{#    {% if is_incremental() -%}#}
{#        and parse_date('%Y%m%d', event_date) >= date_sub(current_date(), interval 2 day)#}
{#    {%- endif -%}#}
),

add_card_value AS (
    select
        _dlt_parent_id,
        max(if(key = 'engagement_time_msec', value__int_value, null)) as engagement_time_msec,
        max(if(key = 'ga_session_number', value__int_value, null)) as ga_session_number,
        max(if(key = 'page_title', value__string_value, null)) as page_title,
        max(if(key = 'engaged_session_event', value__int_value, null)) as engaged_session_event,
        max(if(key = 'session_engaged', value__string_value, null)) as session_engaged,
        max(if(key = 'ga_session_id', value__int_value, null)) as ga_session_id,
        max(if(key = 'page_location', value__string_value, null)) as page_location
    from {{ source('ga4_full_sample', 'ga4_events__event_params') }}
    group by _dlt_parent_id
),

add_product_item_card as (
    select
        _dlt_parent_id as product_row_id,
        item_id as product_id,
        item_name as product_name,
        item_brand as product_brand,
        item_variant as product_variant,
        item_category as product_category,
        price as product_price,
        price_in_usd as product_price_usd,
        quantity as quantity_purchased

    from {{ source('ga4_full_sample', 'ga4_events__items') }}
)

select
    add_card_at,
    profile_id,
    session_id,
    first_seen_at,
    landing_page_title,
    landing_page_url,
    session_number,
    product_id,
    product_name,
    product_brand,
    product_variant,
    product_category,
    quantity_purchased
from (
    select
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(ac.event_timestamp), SECOND)) AS add_card_at,
        ac.user_pseudo_id as profile_id,
        format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(ac.user_first_touch_timestamp), SECOND)) AS first_seen_at,

        ev.page_title as landing_page_title,
        ev.page_location as landing_page_url,
        ev.ga_session_id as session_id,
        ev.ga_session_number as session_number,

        pi.product_id,
        pi.product_name,
        pi.product_brand,
        pi.product_variant,
        pi.product_category,
        pi.quantity_purchased,

        row_number() over(
            partition by
                ac.user_pseudo_id, ev.ga_session_id, pi.product_id
            order by
                ac.event_timestamp
        ) as dedup_row

    from add_card_base_fields ac
    left join add_card_value ev
        on ac._dlt_id = ev._dlt_parent_id
    left join add_product_item_card pi
        on ac._dlt_id = pi.product_row_id
)
    where dedup_row = 1
