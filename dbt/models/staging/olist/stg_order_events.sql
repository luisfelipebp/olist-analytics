with order_events as (
    select
        event_id::uuid,
        event_type::varchar,
        order_id::varchar,
        customer_id::varchar,
        total_value::numeric,
        event_timestamp::timestamp,
        ingested_at::timestamp,
        kafka_offset::bigint,
        kafka_partition::int
    from {{ source('streaming_olist', 'olist_events') }}
)

select *
from order_events