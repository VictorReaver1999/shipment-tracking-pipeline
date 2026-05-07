with shipment_stages as (
    select
        shipment_id,
        carrier,
        region,
        event_type,
        event_timestamp,
        -- get the timestamp of the NEXT event for this shipment
        lead(event_timestamp) over (
            partition by shipment_id
            order by event_timestamp
        ) as next_event_timestamp
    from {{ ref('stg_shipment_events') }}
),

stage_durations as (
    select
        shipment_id,
        carrier,
        region,
        event_type,
        event_timestamp,
        next_event_timestamp,
        -- time spent in this stage = next event timestamp minus current
        extract(epoch from (next_event_timestamp - event_timestamp)) / 60 as stage_duration_minutes
    from shipment_stages
    where next_event_timestamp is not null  -- last event has no next event, exclude it
),

final as (
    select
        event_type                                          as stage,
        carrier,
        region,
        count(shipment_id)                                  as total_shipments,
        round(avg(stage_duration_minutes)::numeric, 2)     as avg_stage_duration_minutes,
        round(min(stage_duration_minutes)::numeric, 2)     as min_stage_duration_minutes,
        round(max(stage_duration_minutes)::numeric, 2)     as max_stage_duration_minutes
    from stage_durations
    group by event_type, carrier, region
)

select * from final