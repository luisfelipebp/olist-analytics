    with orders_metrics as ( 
        select
            *,
            CASE
                when order_approved_at > order_delivered_customer_date then null
                else extract(epoch from (order_delivered_customer_date - order_approved_at)) / 86400.0 
            end as delivery_days
        from {{ref('stg_olist_orders')}}
    )

    select * from orders_metrics