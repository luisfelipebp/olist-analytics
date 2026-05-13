with orders as (
    select customer_state, delivery_days from {{ref('mart_orders')}}
)

select customer_state, avg(delivery_days) as avg_delivery_days from orders group by customer_state