with add_card_base_fields as (
    select * from {{ source('ga4_full_sample', 'ga4_events') }}
    where event_name = 'add_to_cart'
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
    add_card_first_seen_at,
    add_card_landing_page_title,
    add_card_landing_page_url,
    session_number,
    product_id,
    product_name,
    product_brand,
    product_variant,
    product_category,
    quantity_purchased
from (
    select
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(ac.event_timestamp), second)
        ) as add_card_at,
        ac.user_pseudo_id as profile_id,
        format_timestamp(
                '%Y-%m-%d %H:%M:%S', timestamp_trunc(timestamp_micros(ac.user_first_touch_timestamp), second)
        ) as add_card_first_seen_at,

        ev.page_title as add_card_landing_page_title,
        ev.page_location as add_card_landing_page_url,
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
    left join {{ ref('base_event_value') }} ev
        on ac._dlt_id = ev._dlt_parent_id
    left join add_product_item_card pi
        on ac._dlt_id = pi.product_row_id
)
    where dedup_row = 1
