WITH orders AS (

    SELECT * 
    FROM {{ ref('int_orders') }}

),

payments AS (

    SELECT
        order_id,
        SUM(payment_value) AS payment_value
    FROM {{ ref('stg_olist_order_payments') }}
    GROUP BY order_id

),

metrics AS (

    SELECT * 
    FROM {{ ref('int_orders_metrics') }}

),

dim_customer AS (

    SELECT * 
    FROM {{ ref('dim_customers') }}

),

dim_date as (
    select date_day, date_sk
    from {{ ref('dim_date') }}
),

joined AS (

    SELECT
        
        orders.order_id,
        orders.customer_unique_id,
        orders.order_status,
        CAST(orders.order_purchase_timestamp AS DATE) AS order_purchase_date,
    

        payments.payment_value,

        metrics.delivery_days

    FROM orders

    LEFT JOIN payments
        ON orders.order_id = payments.order_id

    LEFT JOIN metrics
        ON orders.order_id = metrics.order_id

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_sk,

        customer_sk,

        date_sk,
        

        order_id,

        order_status,

        payment_value,

        delivery_days

    FROM joined
    left join dim_customer
    on joined.customer_unique_id = dim_customer.customer_unique_id
    left join dim_date
    on joined.order_purchase_date = dim_date.date_day

)

SELECT *
FROM final