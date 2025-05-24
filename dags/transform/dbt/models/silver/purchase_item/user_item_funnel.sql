
select
    vi.view_product_started_at,
    vi.profile_id,
    vi.session_id,
    vi.first_seen_at,
    vi.device_type,
    vi.device_brand,
    vi.device_model,
    vi.os_name,
    vi.os_version,
    vi.browser_name,
    vi.landing_page_title,
    vi.landing_page_url,
    vi.session_number,
    vi.product_id,
    coalesce(pi.product_name,vi.product_name) as product_name,
    coalesce(pi.product_brand,vi.product_brand) as product_brand,
    coalesce(pi.product_variant,vi.product_variant) as product_variant,
    coalesce(pi.product_category,vi.product_category) as product_category,

    if(ac.add_card_at is not null, 1, 0) as add_card,
    if(pi.purchase_completed_at is not null, 1, 0) as purchase_completed,

    ac.add_card_at,
    pi.purchase_completed_at,
    pi.transaction_id,

    vi.product_id as view_id_product,
    pi.product_id as purchase_id_product,

    vi.product_name as v_product_name,
    pi.product_name as p_product_name,

    vi.product_brand as v_product_brand,
    pi.product_brand as p_product_brand,

    vi.product_variant as v_product_variant,
    pi.product_variant as p_product_variant,

    vi.product_category as v_product_category,
    pi.product_category as p_product_category,

    pi.payment_method,
    pi.shipping_method,
    pi.transaction_currency,
    pi.applied_coupon,

    pi.ecommerce__transaction_id,
    pi.unique_items_count,
    pi.event_value_in_usd,
    pi.total_item_quantity,
    pi.purchase_revenue_usd,
    pi.purchase_revenue,
    pi.tax,
    pi.tax_usd,

    pi.product_price,
    pi.product_price_usd,

    pi.quantity_purchased,
    pi.product_revenue,
    pi.product_revenue_usd,

    pi.user_ltv_revenue,
    pi.user_ltv_currency

from {{ ref('user_view_item') }} vi
left join {{ ref('item_add_card') }} ac
    on vi.profile_id = ac.profile_id
    and vi.session_id = ac.session_id
    and vi.product_id = ac.product_id
left join {{ ref('user_purchase_item') }} pi
    on vi.profile_id = pi.profile_id
    and vi.session_id = pi.session_id
    and vi.product_name = pi.product_name