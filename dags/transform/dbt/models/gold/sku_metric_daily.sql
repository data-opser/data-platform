select
    date_trunc(cast(purchase_completed_at as timestamp), day) as as_of_day,
    sku_product_id,
    product_id,
    product_name,
    product_brand,
    product_variant,
    product_category,

    count(distinct transaction_id) as transaction_count,
    sum(ifnull(unique_items_count, 0)) as unique_items_count,
    sum(ifnull(total_item_quantity, 0)) as total_item_quantity,
    sum(ifnull(quantity_purchased, 0)) as quantity_purchased,

    sum(ifnull(product_revenue_usd, 0)) as product_revenue_usd

from {{ ref('user_item_funnel') }}
where purchase_completed = 1
group by 1,2,3,4,5,6,7
