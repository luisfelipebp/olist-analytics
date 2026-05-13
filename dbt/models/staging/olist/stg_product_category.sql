with stg_product_category as (
    select 
        product_category_name::varchar,
        product_category_name_english::varchar
    from {{ source('raw_olist', 'product_category_name_translation') }}
)

select * from stg_product_category