-- Pivot shipment events from multiple rows per shipment
-- into a single row with created and delivered timestamps
with shipment_stages as (
    select
        shipment_id,
        max(carrier) as carrier,
        max(region) as region,
        max(event_day) as event_day,
        min(case when event_type = 'order_created' then event_timestamp end) as created_at,
        max(case when event_type = 'delivered' then event_timestamp end) as delivered_at
    from {{ ref('stg_shipment_events') }}
    group by shipment_id
),

-- Calculate delivery duration in minutes for completed shipments only
delivery_times as (
    select
        shipment_id,
        carrier,
        region,
        event_day,
        created_at,
        delivered_at,
        -- extract(epoch) converts interval to seconds, divide by 60 for minutes
        extract(epoch from (delivered_at - created_at)) / 60 as delivery_minutes
    from shipment_stages
    -- only include shipments that have both a start and end timestamp
    -- failed or in-transit shipments are excluded to avoid skewing averages
    where delivered_at is not null
      and created_at is not null
),

-- Aggregate delivery metrics by carrier, region, and day
final as (
    select
        carrier,
        region,
        event_day,
        count(shipment_id)                          as total_shipments,
        -- round to 2 decimal places, cast to numeric required by PostgreSQL round()
        round(avg(delivery_minutes)::numeric, 2)   as avg_delivery_minutes,
        round(min(delivery_minutes)::numeric, 2)   as min_delivery_minutes,
        round(max(delivery_minutes)::numeric, 2)   as max_delivery_minutes
    from delivery_times
    group by carrier, region, event_day
)

select * from final