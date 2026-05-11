with stg_order_items as (
    select 
        order_id::varchar,
        order_item_id::int,
        product_id::varchar,
        seller_id::varchar,
        shipping_limit_date::timestamp,
        price::numeric,
        freight_value::numeric
    
     from {{ source('raw_olist', 'olist_order_items') }}
)

select * from stg_order_items