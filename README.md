# Shipment Tracking Pipeline

An end-to-end real-time data engineering pipeline that simulates, ingests, processes, stores, and analyzes shipment lifecycle events. Built as a capstone project for the neuefische Data Engineering Bootcamp.

---

## What it does

The pipeline models a realistic logistics system where shipments move through distinct stages -- from order creation to final delivery -- and surfaces analytics on delivery performance, failure rates, and operational bottlenecks.

A Python producer generates synthetic shipment events and publishes them to Kafka. A Python consumer reads those events and writes them to PostgreSQL. dbt transforms the raw data into clean staging models and aggregated mart tables. Airflow orchestrates the dbt runs on an hourly schedule. A Streamlit dashboard visualizes the analytics in real time. AWS S3 archives raw events, provisioned entirely with Terraform.

---

## Architecture

```
Python Producer
      |
      v
    Kafka (KRaft mode)
      |
      v
Python Consumer
      |
      v
  PostgreSQL
  (raw events)
      |
      v
  dbt staging --> dbt marts
                  - delivery times
                  - delay rates
                  - bottlenecks
      |
      v
Streamlit Dashboard

Airflow          -- orchestrates dbt runs hourly
GitHub Actions   -- runs dbt tests on every push
Terraform        -- provisions AWS S3 bucket
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Event streaming | Apache Kafka (KRaft mode) |
| Ingestion | Python + kafka-python-ng |
| Storage | PostgreSQL 16 |
| Transformation | dbt (dbt-postgres) |
| Orchestration | Apache Airflow 2.9.1 |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
| Infrastructure | Terraform + AWS S3 |
| Containerisation | Docker + Docker Compose |

---

## Project structure

```
shipment-tracking-pipeline/
├── producer/
│   └── producer.py          # Generates synthetic shipment events and publishes to Kafka
├── consumer/
│   └── consumer.py          # Reads events from Kafka and writes to PostgreSQL
├── dbt/
│   └── shipment_tracking_pipeline/
│       └── models/
│           ├── staging/
│           │   ├── sources.yml              # Source definitions and data quality tests
│           │   └── stg_shipment_events.sql  # Cleans and types raw events
│           └── marts/
│               ├── mart_delivery_times.sql  # Avg delivery time per carrier and region
│               ├── mart_delay_rates.sql     # Failure rates per carrier and region
│               └── mart_bottlenecks.sql     # Time spent per lifecycle stage
├── airflow/
│   ├── Dockerfile
│   ├── profiles.yml          # dbt connection config for inside the Airflow container
│   └── dags/
│       └── dbt_pipeline.py   # Hourly DAG: dbt run then dbt test
├── streamlit/
│   ├── Dockerfile
│   └── app.py                # Dashboard connecting to PostgreSQL mart views
├── terraform/
│   ├── main.tf               # AWS provider config
│   ├── variables.tf          # Region, project name, environment
│   ├── s3.tf                 # S3 bucket with versioning and lifecycle rules
│   └── outputs.tf            # Bucket name and ARN
├── .github/
│   └── workflows/
│       └── dbt_tests.yml     # Runs dbt run and dbt test on every push to main
├── docker-compose.yml        # Spins up Kafka, PostgreSQL, Airflow, and Streamlit
├── .env.example              # Template for required environment variables
└── requirements.txt          # Python dependencies
```

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- Terraform (for AWS provisioning)
- AWS account with credentials configured (for Terraform only)

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/VictorReaver1999/shipment-tracking-pipeline.git
cd shipment-tracking-pipeline
```

### 2. Create your environment file

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```
POSTGRES_DB=logistics
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

### 3. Create a Python virtual environment

```bash
python -m venv venv
source venv/Scripts/activate  # Windows
# source venv/bin/activate    # Mac/Linux
pip install -r requirements.txt
```

### 4. Start the stack

```bash
docker compose up -d
```

This starts Kafka, PostgreSQL, Airflow, and Streamlit in one command.

### 5. Create the Kafka topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic shipment-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 6. Create the Airflow database

```bash
docker exec -it postgres psql -U your_username -d logistics -c "CREATE DATABASE airflow;"
```

---

## Running the pipeline

### Generate and ingest events

Open two terminals, both with the virtual environment activated.

Terminal 1 -- start the consumer first:
```bash
python consumer/consumer.py
```

Terminal 2 -- start the producer:
```bash
python producer/producer.py
```

The producer generates synthetic shipment events and publishes them to Kafka. The consumer reads them and writes to PostgreSQL. Stop both with Ctrl+C when you have enough data.

### Run dbt transformations manually

```bash
cd dbt/shipment_tracking_pipeline
dbt run
dbt test
```

### Query the marts directly

```bash
# Delivery times by carrier and region
docker exec -it postgres psql -U your_username -d logistics \
  -c "SELECT * FROM dbt_dev.mart_delivery_times LIMIT 10;"

# Failure rates by carrier and region
docker exec -it postgres psql -U your_username -d logistics \
  -c "SELECT * FROM dbt_dev.mart_delay_rates LIMIT 10;"

# Bottlenecks by lifecycle stage
docker exec -it postgres psql -U your_username -d logistics \
  -c "SELECT * FROM dbt_dev.mart_bottlenecks LIMIT 10;"
```

---

## Accessing the services

| Service | URL | Credentials |
|---|---|---|
| Streamlit dashboard | http://localhost:8501 | None required |
| Airflow UI | http://localhost:8080 | admin / (see logs) |

To get the Airflow password:
```bash
docker logs airflow 2>&1 | grep password
```

---

## Airflow orchestration

The `dbt_shipment_pipeline` DAG runs automatically every hour. It executes two tasks in sequence:

1. `dbt_run` -- rebuilds all four dbt models
2. `dbt_test` -- runs all nine data quality tests

If `dbt_run` fails, `dbt_test` is skipped. Failed tasks retry once after five minutes.

To trigger a manual run, open the Airflow UI at http://localhost:8080, find the DAG, and click the play button.

---

## CI/CD

Every push to the main branch triggers the GitHub Actions workflow in `.github/workflows/dbt_tests.yml`. It spins up a fresh Ubuntu environment, installs dbt, seeds a test PostgreSQL database, runs `dbt run`, and then `dbt test`. A green checkmark on the commit means the transformation layer is working correctly.

---

## AWS infrastructure (Terraform)

The S3 bucket for raw event archiving is provisioned with Terraform. To apply:

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates a private S3 bucket in `eu-central-1` with versioning enabled and automatic lifecycle rules that move data to cheaper storage tiers after 30 and 90 days.

Credentials are read from your local AWS CLI configuration. Never commit `.tfstate` files -- they are in `.gitignore`.

---

## Data model

### Raw table: `public.shipment_events`

| Column | Type | Description |
|---|---|---|
| id | SERIAL | Auto-incrementing primary key |
| shipment_id | UUID | Unique identifier per shipment |
| event_type | VARCHAR | order_created, picked_up, in_transit, out_for_delivery, delivered, failed |
| timestamp | TIMESTAMPTZ | When the event occurred |
| location | VARCHAR | City where the event happened |
| carrier | VARCHAR | DHL, UPS, FedEx, Hermes, DPD |
| region | VARCHAR | North, South, East, West, Central |
| ingested_at | TIMESTAMPTZ | When the pipeline received the event |

### Staging: `dbt_dev.stg_shipment_events`

Cleaned, typed, and enriched version of the raw table. Filters out NULL shipment IDs, invalid event types, and future timestamps. Adds derived fields: `ingestion_delay`, `event_hour`, `event_day`.

### Marts

**`dbt_dev.mart_delivery_times`** -- average, minimum, and maximum delivery time in minutes per carrier, region, and day. Only includes shipments that completed successfully.

**`dbt_dev.mart_delay_rates`** -- total shipments, failed shipments, delivered shipments, failure rate percentage, and delivery rate percentage per carrier, region, and day.

**`dbt_dev.mart_bottlenecks`** -- average time spent in each lifecycle stage per carrier and region. Uses the LEAD() window function to calculate the gap between consecutive events.

---

## Data quality

The producer intentionally corrupts 3% of events to simulate real-world data quality issues: NULL shipment IDs, invalid event type strings, and timestamps set in the future.

The consumer handles NULL shipment IDs by catching the PostgreSQL constraint violation, rolling back the transaction, logging the bad event, and continuing. It never crashes on bad input.

The dbt staging model filters out invalid event types and future timestamps before they reach the mart layer. The `accepted_values` test on the raw table flags the corrupted records and intentionally fails -- this proves dirty data exists at source and confirms the staging filter is working correctly.

---

## Shutting down

```bash
docker compose down
```

To also remove all data volumes (wipes the database):
```bash
docker compose down -v
```

---

## Capstone project period

21 April 2026 -- 21 May 2026
neuefische GmbH Data Engineering Bootcamp
