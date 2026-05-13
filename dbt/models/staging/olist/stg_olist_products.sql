with stg_products as (
    select 
    product_id::varchar,
    COALESCE(product_category_name::varchar, 'produto_sem_nome') as product_category_name,
    product_name_lenght::int as product_name_length,
    product_description_lenght::int as product_description_length,
    product_photos_qty::int,
    product_weight_g::int,
    product_length_cm::int,
    product_height_cm::int,
    product_width_cm::int
    from {{source('raw_olist', 'olist_products')}}
)

select * from stg_products