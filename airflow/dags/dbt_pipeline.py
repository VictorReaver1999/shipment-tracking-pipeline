from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- Default arguments applied to every task in the DAG ---
default_args = {
    "owner": "victor",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# --- DAG definition ---
with DAG(
    dag_id="dbt_shipment_pipeline",
    description="Run dbt transformations on shipment data",
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule_interval="@hourly",
    catchup=False,
    tags=["dbt", "logistics", "shipment"],
) as dag:

    # Task 1: run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/shipment_tracking_pipeline && dbt run --profiles-dir /opt/airflow/dbt/shipment_tracking_pipeline",
    )

    # Task 2: run dbt tests — only runs if dbt_run succeeds
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/shipment_tracking_pipeline && dbt test --profiles-dir /opt/airflow/dbt/shipment_tracking_pipeline",
    )

    # Define dependency: dbt_run must succeed before dbt_test runs
    dbt_run >> dbt_test