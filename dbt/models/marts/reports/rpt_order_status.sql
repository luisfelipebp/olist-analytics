with orders as (
    select order_status, order_sk from {{ ref('mart_orders')}}
)

select order_status, count(distinct order_sk) from orders group by order_status