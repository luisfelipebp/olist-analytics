with order_items as (
    select * from {{ref('fct_order_items')}}
),
products as (
    select * from {{ref('dim_products')}}
),
final as (
    select 
        o.order_sk,
        p.product_category_name,
        o.price,
        o.freight_value,
        o.delivery_days
    from order_items o 
    inner join products p
    on o.product_sk = p.product_sk
)

SELECT * FROM final
