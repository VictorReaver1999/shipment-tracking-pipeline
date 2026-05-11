import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# --- Config ---
KAFKA_TOPIC = "shipment-events"
KAFKA_BROKER = "localhost:9092"

# --- Simulated data pools ---
CARRIERS = ["DHL", "UPS", "FedEx", "Hermes", "DPD"]
# REGIONS = ["North", "South", "East", "West", "Central"]
# CITIES = ["Hamburg", "Berlin", "Munich", "Frankfurt", "Cologne", "Stuttgart", "Leipzig"]
# EVENT_TYPES = ["order_created", "picked_up", "in_transit", "out_for_delivery", "delivered", "failed"]

CITY_REGION_MAP = {
    "Hamburg": "North",
    "Berlin": "East",
    "Munich": "South",
    "Stuttgart": "South",
    "Frankfurt": "Central",
    "Cologne": "West",
    "Leipzig": "East"
}

def introduce_errors(event):
    """
    Randomly corrupt 3% of events to simulate real-world data quality issues.
    Each error type is applied independently so multiple errors can co-exist.
    """
    # 3% chance of NULL shipment_id
    if random.random() < 0.03:
        event["shipment_id"] = None

    # 3% chance of invalid event type (typo)
    if random.random() < 0.03:
        event["event_type"] = random.choice(["deliverd", "pickd_up", "FAILED", "transit"])

    # 3% chance of timestamp in the future (corrupted clock)
    if random.random() < 0.03:
        from datetime import timedelta
        event["timestamp"] = (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))).isoformat()

    return event

# Realistic time offsets per stage in hours
# Simulates a real logistics timeline: order to delivery takes 2-3 days
STAGE_OFFSETS_HOURS = {
    "order_created":      0,
    "picked_up":          random.randint(2, 8),
    "in_transit":         random.randint(12, 36),
    "out_for_delivery":   random.randint(48, 60),
    "delivered":          random.randint(50, 72),
    "failed":             random.randint(24, 72),
}

def generate_event(shipment_id, event_type, city, carrier, offset_hours=0):
    # Base time 4 days ago so all events including delivered fall in the past
    base_time = datetime.now(timezone.utc) - timedelta(hours=96)
    event_time = base_time + timedelta(hours=offset_hours)
    event = {
        "shipment_id": shipment_id,
        "event_type": event_type,
        "timestamp": event_time.isoformat(),
        "location": city,
        "carrier": carrier,
        "region": CITY_REGION_MAP[city]
    }
    return introduce_errors(event)

def simulate_shipment():
    shipment_id = str(uuid.uuid4())
    city = random.choice(list(CITY_REGION_MAP.keys()))
    carrier = random.choice(CARRIERS)

    # Generate realistic offsets once per shipment
    picked_up_hours = random.randint(2, 8)
    in_transit_hours = picked_up_hours + random.randint(10, 28)
    out_hours = in_transit_hours + random.randint(12, 24)
    delivered_hours = out_hours + random.randint(2, 12)

    stage_offsets = {
        "order_created": 0,
        "picked_up": picked_up_hours,
        "in_transit": in_transit_hours,
        "out_for_delivery": out_hours,
        "delivered": delivered_hours,
        "failed": in_transit_hours + random.randint(2, 12),
    }

    if random.random() < 0.1:
        stages = ["order_created", "picked_up", "in_transit", "failed"]
    else:
        stages = ["order_created", "picked_up", "in_transit", "out_for_delivery", "delivered"]

    for event_type in stages:
        event = generate_event(shipment_id, event_type, city, carrier, stage_offsets[event_type])
        yield event
        time.sleep(random.uniform(0.5, 2.0))

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Producer started. Sending events to topic: {KAFKA_TOPIC}")

    while True:
        for event in simulate_shipment():
            producer.send(KAFKA_TOPIC, value=event)
            print(f"Sent: {event}")
        time.sleep(1)  # short pause between shipments

if __name__ == "__main__":
    main()