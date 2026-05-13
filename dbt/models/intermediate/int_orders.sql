WITH orders AS (
    SELECT * FROM {{ ref('stg_olist_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_olist_customers') }}
),

final AS (
    SELECT 
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date

    FROM orders o

    INNER JOIN customers c
        ON o.customer_id = c.customer_id
)

SELECT * FROM final