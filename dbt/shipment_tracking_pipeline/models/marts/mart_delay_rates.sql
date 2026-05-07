with shipment_outcomes as (
    select
        shipment_id,
        carrier,
        region,
        event_day,
        -- flag whether this shipment failed
        max(case when event_type = 'failed' then 1 else 0 end)     as is_failed,
        -- flag whether this shipment was delivered
        max(case when event_type = 'delivered' then 1 else 0 end)   as is_delivered
    from {{ ref('stg_shipment_events') }}
    group by shipment_id, carrier, region, event_day
),

final as (
    select
        carrier,
        region,
        event_day,
        count(shipment_id)                                              as total_shipments,
        sum(is_failed)                                                  as failed_shipments,
        sum(is_delivered)                                               as delivered_shipments,
        round(sum(is_failed)::numeric / count(shipment_id) * 100, 2)   as failure_rate_pct,
        round(sum(is_delivered)::numeric / count(shipment_id) * 100, 2) as delivery_rate_pct
    from shipment_outcomes
    group by carrier, region, event_day
)

select * from final