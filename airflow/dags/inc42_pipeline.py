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
        "MYSQL_HOST": "inc42-prod.mysql.database.azure.com",
        "MYSQL_USER": "{{ var.value.MYSQL_USER }}",
        "MYSQL_PASSWORD": "{{ var.value.MYSQL_PASSWORD }}",
        "PG_HOST": "datalabsdata.postgres.database.azure.com",
        "PG_USER": "{{ var.value.PG_USER }}",
        "PG_PASSWORD": "{{ var.value.PG_PASSWORD }}",
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
    # Customer.io: load parquet files from GCS → BigQuery Bronze
    ingest_customerio = BashOperator(
        task_id="load_customerio_gcs_to_bq",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/customerio_gcs_to_bq.py",
        doc="Load Customer.io parquet files from GCS into 9 Bronze tables (2.4M+ rows)",
    )

    # Tally: fetch all form submissions via API → BigQuery Bronze
    ingest_tally = BashOperator(
        task_id="load_tally_forms",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/tally_ingest.py",
        doc="Fetch all Tally form submissions via REST API → bronze.tally_forms",
    )

    # Inc42 DB: users + usermeta → BigQuery Bronze
    ingest_inc42_db = BashOperator(
        task_id="load_inc42_db",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/inc42_db_ingest.py",
        doc="Load 42_users and 42_usermeta from MySQL → bronze.inc42_users, bronze.inc42_usermeta",
    )

    # Gravity Forms: entries + entry_meta → BigQuery Bronze
    ingest_gravity = BashOperator(
        task_id="load_gravity_forms",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/gravity_forms_ingest.py",
        doc="Load 42_gf_entry and 42_gf_entry_meta from MySQL → bronze.gravity_forms_*",
    )

    # WooCommerce: orders, order_meta, products, order_items → BigQuery Bronze
    ingest_woocommerce = BashOperator(
        task_id="load_woocommerce",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/woocommerce_ingest.py",
        doc="Load WooCommerce tables from MySQL → bronze.woocommerce_*",
    )

    # Datalabs: 30 company tables from PostgreSQL → BigQuery Bronze
    ingest_datalabs = BashOperator(
        task_id="load_datalabs",
        bash_command=f"python {PROJECT_DIR}/ingestion/scripts/datalabs_ingest.py",
        doc="Load 30 Datalabs tables from PostgreSQL → bronze.dl_*",
    )

    # Future:
    # ingest_hubspot = BashOperator(...)

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

    dbt_run = BashOperator(
        task_id="dbt_run_all",
        bash_command=f"cd {PROJECT_DIR}/dbt/inc42_warehouse && dbt run",
        doc="Run all dbt models: Silver (contacts, companies) → Gold (dims, facts, 360 views)",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR}/dbt/inc42_warehouse && dbt test",
        doc="Run dbt tests: uniqueness, not_null checks on key columns",
    )

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

    # Phase 1: All ingestion runs in parallel
    # Phase 2: PySpark runs after ALL ingestion completes
    # Phase 1 → Phase 2 → Phase 3
    [ingest_bronze, ingest_customerio, ingest_tally, ingest_inc42_db, ingest_gravity, ingest_woocommerce, ingest_datalabs] >> spark_identity
    spark_identity >> dbt_run >> dbt_test

    # Future:
    # dbt_test >> [retl_customerio, retl_slack, retl_sheets]
