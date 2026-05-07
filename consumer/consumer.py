import json
import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load variables from .env file into the environment
# Must be called before any os.getenv() calls
load_dotenv()

# --- Config ---
KAFKA_TOPIC = "shipment-events"

# Read broker address from .env, fall back to localhost if not set
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# PostgreSQL connection details read from .env
# Never hardcode credentials — they stay in .env which is gitignored
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432
}

def get_connection():
    """Open and return a connection to PostgreSQL using DB_CONFIG."""
    return psycopg2.connect(**DB_CONFIG)

def create_table(conn):
    """
    Create the shipment_events table if it doesn't already exist.
    Safe to run on every startup — IF NOT EXISTS prevents errors.
    
    Columns:
        id           -- auto-incrementing primary key
        shipment_id  -- UUID matching the producer's shipment identifier
        event_type   -- stage in the shipment lifecycle
        timestamp    -- when the event occurred (timezone-aware)
        location     -- city where the event happened
        carrier      -- logistics carrier (DHL, UPS etc.)
        region       -- geographic region derived from city
        ingested_at  -- when WE received the event (auto-set to NOW())
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shipment_events (
                id SERIAL PRIMARY KEY,
                shipment_id UUID NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                location VARCHAR(100),
                carrier VARCHAR(50),
                region VARCHAR(50),
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()  # commit saves the change permanently

def insert_event(conn, event):
    """
    Insert a single shipment event dictionary into the database.
    
    Uses %(field)s syntax for safe parameterised queries.
    Never use f-strings or string concatenation to build SQL —
    that opens the door to SQL injection attacks.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO shipment_events 
                (shipment_id, event_type, timestamp, location, carrier, region)
            VALUES 
                (%(shipment_id)s, %(event_type)s, %(timestamp)s, %(location)s, %(carrier)s, %(region)s)
        """, event)
        conn.commit()

def main():
    # Open a single persistent connection — stays open for the life of the consumer
    conn = get_connection()
    
    # Ensure table exists before we try to insert anything
    create_table(conn)
    
    print("Consumer started. Listening for events...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        # Deserializer: reverse of producer's serializer
        # bytes -> decoded string -> Python dictionary
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # If this consumer has never run before, read from the
        # very first message in the topic — don't skip anything
        auto_offset_reset="earliest",
        # Group ID lets Kafka track what this consumer has already processed
        # If it crashes and restarts, it picks up where it left off
        group_id="shipment-consumer-group"
    )

    # Infinite loop — blocks here waiting for new messages
    # Each iteration processes one event from Kafka
    for message in consumer:
        event = message.value
        try:
            insert_event(conn, event)
            print(f"Inserted: {event}")
        except Exception as e:
            conn.rollback()  # reset the failed transaction
            print(f"Skipped bad event: {e} | Event: {event}")

if __name__ == "__main__":
    main()