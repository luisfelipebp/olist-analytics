WITH seller AS ( 
    SELECT * FROM {{ ref('stg_olist_sellers') }}
),
final AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_sk,
        seller.seller_id,
        seller.seller_zip_code_prefix,
        seller.seller_city,
        seller.seller_state
        from seller
    )

SELECT * FROM final