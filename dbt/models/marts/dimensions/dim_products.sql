WITH products AS ( 
    SELECT * FROM {{ ref('stg_olist_products') }}
),

category AS (
    SELECT * FROM {{ ref('stg_product_category') }}
),

final AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['products.product_id']) }} AS product_sk,
        products.product_id,
        products.product_category_name,
        category.product_category_name_english,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm

    FROM products
    LEFT JOIN category
        ON products.product_category_name = category.product_category_name
)

SELECT * FROM final