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
EVENT_TYPES = ["order_created", "picked_up", "in_transit", "out_for_delivery", "delivered", "failed"]

CITY_REGION_MAP = {
    "Hamburg": "North",
    "Berlin": "East",
    "Munich": "South",
    "Stuttgart": "South",
    "Frankfurt": "Central",
    "Cologne": "West",
    "Leipzig": "East"
}

def generate_event(shipment_id, event_type):
    city = random.choice(list(CITY_REGION_MAP.keys()))
    return {
        "shipment_id": shipment_id,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": city,
        "carrier": random.choice(CARRIERS),
        "region": CITY_REGION_MAP[city]
    }

def simulate_shipment():
    shipment_id = str(uuid.uuid4())
    # Each shipment progresses through stages in order
    for event_type in EVENT_TYPES:
        event = generate_event(shipment_id, event_type)
        yield event
        time.sleep(random.uniform(0.5, 2.0))  # simulate time between events

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