with stg_customers as (
    select 
    customer_id::varchar,
    customer_unique_id::varchar,
    customer_zip_code_prefix::varchar(5),
    customer_city::varchar,
    customer_state::varchar
    from {{source('raw_olist', 'olist_customers')}}
)

select * from stg_customers