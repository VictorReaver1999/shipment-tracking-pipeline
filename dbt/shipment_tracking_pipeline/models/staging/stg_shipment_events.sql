with source as (
    select * from {{ source('logistics', 'shipment_events') }}
),

staged as (
    select
        -- identifiers
        id                                      as event_id,
        shipment_id::text                       as shipment_id,

        -- event details
        event_type,
        location                                as city,
        carrier,
        region,

        -- timestamps
        timestamp at time zone 'UTC'            as event_timestamp,
        ingested_at at time zone 'UTC'          as ingested_at,

        -- derived fields
        (ingested_at - timestamp)               as ingestion_delay,
        date_trunc('hour', timestamp)           as event_hour,
        date_trunc('day', timestamp)            as event_day

    from source
    where event_type is not null
      and shipment_id is not null
      and event_type in ('order_created', 'picked_up', 'in_transit', 'out_for_delivery', 'delivered', 'failed')
      and timestamp <= now()  -- filter out future timestamps
)

select * from staged