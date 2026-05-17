import json
import os
import uuid
import boto3
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load variables from .env file into the environment
# Must be called before any os.getenv() calls
load_dotenv()

# --- S3 config ---
# Read bucket name from .env -- falls back to the known bucket name
S3_BUCKET = os.getenv("S3_BUCKET", "shipment-tracking-events-dev-279091550367")
S3_PREFIX = "raw-events"

# Initialise S3 client using local AWS CLI credentials
s3_client = boto3.client("s3", region_name="eu-central-1")

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

def archive_to_s3(event):
    """
    Archive a single shipment event to S3 as a JSON file.
    
    Each event is stored as an individual JSON file under:
    raw-events/YYYY-MM-DD/shipment_id/event_type_uuid.json
    
    This mirrors how real data lakes organise raw event archives --
    partitioned by date for efficient querying with Athena.
    """
    try:
        # Use event date for partitioning -- standard data lake pattern
        event_date = event["timestamp"][:10]  # YYYY-MM-DD
        key = f"{S3_PREFIX}/{event_date}/{event['shipment_id']}/{event['event_type']}_{uuid.uuid4()}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(event),
            ContentType="application/json"
        )
    except Exception as e:
        # Never crash on S3 failure -- PostgreSQL is the primary store
        print(f"S3 archive failed (non-fatal): {e}")

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
            archive_to_s3(event)  # archive to S3 after successful PostgreSQL insert
            print(f"Inserted + archived: {event['event_type']} | {event['shipment_id'][:8]}...")
        except Exception as e:
            conn.rollback()
            print(f"Skipped bad event: {e} | Event: {event}")

if __name__ == "__main__":
    main()