with stg_sellers as (
    select 
    seller_id::varchar,
    seller_zip_code_prefix::varchar(5),
    seller_city::varchar,
    seller_state::varchar(2)
    from {{source('raw_olist', 'olist_sellers')}}
)

select * from stg_sellers