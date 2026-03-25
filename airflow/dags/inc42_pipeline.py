"""
Inc42 Data Warehouse — Main Airflow DAG
Orchestrates: Ingestion → PySpark → dbt → Reverse ETL

Runs daily at midnight IST (18:30 UTC).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# ── Config ──
PROJECT_DIR = "/opt/inc42-data-warehouse"
CREDENTIALS = "/secrets/bq-service-account.json"

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": CREDENTIALS,
        "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    },
}

with DAG(
    dag_id="inc42_data_warehouse",
    default_args=default_args,
    description="Full pipeline: Ingest → PySpark → dbt → Reverse ETL",
    schedule_interval="30 18 * * *",  # midnight IST = 18:30 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["data-warehouse", "production"],
    max_active_runs=1,
) as dag:

    # ════════════════════════════════════════════
    #  PHASE 1: INGESTION (all run in parallel)
    # ════════════════════════════════════════════

    ingest_bronze = BashOperator(
        task_id="load_bronze_tables",
        bash_command=f"python {PROJECT_DIR}/scripts/01_create_bronze_tables.py",
    )

    # When you add real ingestion scripts, replace above with:
    # ingest_woo = BashOperator(
    #     task_id="ingest_woocommerce",
    #     bash_command=f"python {PROJECT_DIR}/ingestion/scripts/woocommerce_ingest.py",
    # )
    # ingest_hubspot = BashOperator(...)
    # ingest_gravity = BashOperator(...)
    # etc.

    # ════════════════════════════════════════════
    #  PHASE 2: PYSPARK (after ingestion)
    # ════════════════════════════════════════════

    spark_identity = BashOperator(
        task_id="spark_identity_resolution",
        bash_command=f"python {PROJECT_DIR}/spark/identity_resolution.py",
    )

    # Future PySpark jobs:
    # spark_company = BashOperator(
    #     task_id="spark_company_resolution",
    #     bash_command=f"python {PROJECT_DIR}/spark/company_resolution.py",
    # )
    # spark_revenue = BashOperator(
    #     task_id="spark_revenue_parser",
    #     bash_command=f"python {PROJECT_DIR}/spark/revenue_parser.py",
    # )

    # ════════════════════════════════════════════
    #  PHASE 3: DBT (after PySpark)
    # ════════════════════════════════════════════

    # dbt_silver = BashOperator(
    #     task_id="dbt_run_silver",
    #     bash_command="cd /opt/dbt/inc42_warehouse && dbt run --select silver.*",
    # )
    #
    # dbt_gold = BashOperator(
    #     task_id="dbt_run_gold",
    #     bash_command="cd /opt/dbt/inc42_warehouse && dbt run --select gold.*",
    # )
    #
    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command="cd /opt/dbt/inc42_warehouse && dbt test",
    # )

    # ════════════════════════════════════════════
    #  PHASE 4: REVERSE ETL (after dbt)
    # ════════════════════════════════════════════

    # retl_customerio = BashOperator(
    #     task_id="push_to_customerio",
    #     bash_command=f"python {PROJECT_DIR}/reverse_etl/push_to_customerio.py",
    # )

    # ════════════════════════════════════════════
    #  DEPENDENCIES
    # ════════════════════════════════════════════

    # Phase 1 → Phase 2 → Phase 3 → Phase 4
    ingest_bronze >> spark_identity

    # When you add more tasks:
    # [ingest_woo, ingest_hubspot, ...] >> spark_identity
    # [spark_identity, spark_company, spark_revenue] >> dbt_silver
    # dbt_silver >> dbt_gold >> dbt_test
    # dbt_test >> [retl_customerio, retl_slack, retl_sheets]
