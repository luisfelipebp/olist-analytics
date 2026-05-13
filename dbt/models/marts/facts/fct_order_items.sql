with items as (
    select * from {{ ref('stg_olist_order_items') }}
),

orders as (
    select order_id, customer_unique_id
    from {{ ref('int_orders') }}
),

metrics as (
    select * from {{ ref('int_orders_metrics') }}
),

dim_customers as (
    select customer_unique_id, customer_sk
    from {{ ref('dim_customers') }}
),

dim_products as (
    select product_id, product_sk
    from {{ ref('dim_products') }}
),

dim_sellers as (
    select seller_id, seller_sk
    from {{ ref('dim_sellers') }}
),

dim_date as (
    select date_day, date_sk
    from {{ ref('dim_date') }}
),

joined as (
    select
        items.order_id,
        items.order_item_id,
        items.product_id,
        items.seller_id,
        items.price,
        items.freight_value,
        items.shipping_limit_date,
        orders.customer_unique_id,
        metrics.delivery_days
    from items
    left join orders  on items.order_id = orders.order_id
    left join metrics on items.order_id = metrics.order_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,

        {{ dbt_utils.generate_surrogate_key(['order_id']) }}                  as order_sk,

        dc.customer_sk,
        dp.product_sk,
        ds.seller_sk,
        dd.date_sk,

        joined.order_id,
        joined.order_item_id,
        joined.price,
        joined.freight_value,
        joined.delivery_days

    from joined
    left join dim_customers dc on joined.customer_unique_id       = dc.customer_unique_id
    left join dim_products  dp on joined.product_id               = dp.product_id
    left join dim_sellers   ds on joined.seller_id                = ds.seller_id
    left join dim_date      dd on cast(joined.shipping_limit_date as date) = dd.date_day
)

select * from final