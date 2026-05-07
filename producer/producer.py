import json
import random
import time
import uuid
from datetime import datetime, timezone
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

def generate_event(shipment_id, event_type, city, carrier):
    event = {
        "shipment_id": shipment_id,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": city,
        "carrier": carrier,
        "region": CITY_REGION_MAP[city]
    }
    return introduce_errors(event)

def simulate_shipment():
    shipment_id = str(uuid.uuid4())
    city = random.choice(list(CITY_REGION_MAP.keys()))
    carrier = random.choice(CARRIERS)
    
    if random.random() < 0.1:
        stages = ["order_created", "picked_up", "in_transit", "failed"]
    else:
        stages = ["order_created", "picked_up", "in_transit", "out_for_delivery", "delivered"]
    
    for event_type in stages:
        event = generate_event(shipment_id, event_type, city, carrier)
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