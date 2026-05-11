WITH payments AS (

    SELECT * 
    FROM {{ ref('stg_olist_order_payments') }}

),

orders AS (

    SELECT * 
    FROM {{ ref('int_orders') }}

),

dim_customers AS (

    SELECT * 
    FROM {{ ref('dim_customers') }}

),

joined AS (

    SELECT
        payments.order_id,

        payments.payment_sequential,

        payments.payment_type,

        payments.payment_installments,

        payments.payment_value,

        orders.customer_unique_id

    FROM payments 

    LEFT JOIN orders
        ON payments.order_id = orders.order_id

),

final AS (

    SELECT 

        {{ dbt_utils.generate_surrogate_key(['joined.order_id', 'joined.payment_sequential']) }} AS order_payment_sk,

        {{ dbt_utils.generate_surrogate_key(['joined.order_id']) }} AS order_sk,

        dc.customer_sk,

        joined.payment_type,

        joined.payment_installments,

        joined.payment_value::numeric(10,2)

    FROM joined

    INNER JOIN dim_customers dc
        ON joined.customer_unique_id = dc.customer_unique_id

)

SELECT *
FROM final