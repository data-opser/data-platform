
with product_items as (
    select
        _dlt_parent_id as product_row_id,
        item_id as product_id,
        item_name as product_name,
        item_brand as product_brand,
        item_variant as product_variant,
        item_category as product_category,
        price as product_price,
        price_in_usd as product_price_usd,
        quantity as quantity_purchased,
        item_revenue as product_revenue,
        item_revenue_in_usd as product_revenue_usd
    from {{ source('ga4_full_sample', 'ga4_events__items') }}
)

select
    tc.purchase_completed_at,
    tc.profile_id,
    tc.session_id,
    tc.transaction_id,

    tc.payment_method,
    tc.shipping_method,
    tc.transaction_currency,
    tc.applied_coupon,

    tc.ecommerce__transaction_id,
    tc.unique_items_count,
    tc.event_value_in_usd,
    tc.total_item_quantity,
    tc.purchase_revenue_usd,
    tc.purchase_revenue,
    tc.tax,
    tc.tax_usd,

    ei.product_id,
    ei.product_name,
    ei.product_brand,
    ei.product_variant,
    ei.product_category,

    ei.product_price,
    ei.product_price_usd,

    ei.quantity_purchased,
    ei.product_revenue,
    ei.product_revenue_usd,

    tc.user_ltv_revenue,
    tc.user_ltv_currency

from {{ ref('user_transaction_completed') }} tc
left join product_items ei
    on tc.event_row_id = ei.product_row_id
