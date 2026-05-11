with stg_geolocation as (
    select 
    geolocation_zip_code_prefix::VARCHAR(5),
    geolocation_lat::double precision,
    geolocation_lng::double precision,
    geolocation_city::VARCHAR,
    geolocation_state::VARCHAR
    from {{source('raw_olist', 'olist_geolocation')}}
)

select * from stg_geolocation