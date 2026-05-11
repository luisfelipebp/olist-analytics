with stg_orders  as (
    select
        order_id::varchar,
        customer_id::varchar,
        order_status::varchar,
        order_purchase_timestamp::timestamp, 
        order_approved_at::timestamp,
        order_delivered_carrier_date::timestamp,
        order_delivered_customer_date::timestamp,
        order_estimated_delivery_date::timestamp
    from {{ source('raw_olist', 'olist_orders') }}
)

select * from stg_orders
