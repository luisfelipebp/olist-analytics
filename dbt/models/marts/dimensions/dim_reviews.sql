WITH reviews AS (

    SELECT * 
    FROM {{ ref('stg_olist_reviews') }}

),

final AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['review_id']) }} AS review_sk,

        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp

    FROM reviews

)

SELECT *
FROM final