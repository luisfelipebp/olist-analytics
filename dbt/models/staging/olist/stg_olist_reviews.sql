with stg_reviews as (
    select 
    review_id::varchar,
    order_id::varchar,
    review_score::int,
    review_comment_title::varchar,
    review_comment_message::varchar,
    review_creation_date::timestamp,
    review_answer_timestamp::timestamp
    from {{source('raw_olist', 'olist_order_reviews')}}
)

select * from stg_reviews