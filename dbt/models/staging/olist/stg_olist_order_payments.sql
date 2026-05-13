with stg_order_payments as (
    select 
        order_id::varchar,
        payment_sequential::int,
        payment_type::varchar,
        payment_installments::int,
        payment_value::numeric(10,2)
     from {{ source('raw_olist', 'olist_order_payments') }}
)

select * from stg_order_payments