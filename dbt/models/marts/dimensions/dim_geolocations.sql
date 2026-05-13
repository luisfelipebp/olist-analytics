with geolocations as (
    select * from {{ref('stg_olist_geolocations')}}
),
final as (
    select 
        {{ dbt_utils.generate_surrogate_key(['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']) }} as geolocation_sk, 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
        from geolocations
)

select * from final