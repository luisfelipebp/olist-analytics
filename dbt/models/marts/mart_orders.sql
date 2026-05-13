WITH orders AS (

    SELECT * 
    FROM {{ ref('fct_orders') }}

),

dim_customers AS (

    SELECT * 
    FROM {{ ref('dim_customers') }}

),

dim_date AS (

    SELECT * 
    FROM {{ ref('dim_date') }}

),

joined AS (

    SELECT
        orders.order_sk,

        dc.customer_unique_id,

        dd.date_day,

        dc.customer_state,

        orders.order_status,

        orders.payment_value,

        orders.delivery_days

    FROM orders

    INNER JOIN dim_customers dc
        ON orders.customer_sk = dc.customer_sk

    INNER JOIN dim_date dd
        ON orders.date_sk = dd.date_sk

)

SELECT *
FROM joined