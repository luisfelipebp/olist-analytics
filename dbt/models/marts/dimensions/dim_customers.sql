WITH customers AS (
    SELECT * FROM {{ ref('stg_olist_customers') }}
),

deduplicated AS (

    SELECT
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY customer_id
        ) AS rn

    FROM customers
),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} AS customer_sk,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state

    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM final