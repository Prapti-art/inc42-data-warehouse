"""
Customer.io GCS → BigQuery Bronze Loader

Customer.io drops parquet files into GCS every 10 minutes.
This script loads them into BigQuery Bronze tables.

Run daily via Airflow. Uses WRITE_TRUNCATE (full refresh) since
Customer.io sends cumulative snapshots, not incremental deltas.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/customerio_gcs_to_bq.py
"""

import os
from google.cloud import bigquery

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")

BQ_PROJECT = "bigquery-296406"
GCS_BUCKET = "gs://inc42-data-warehouse-raw/customerio"

bq = bigquery.Client(project=BQ_PROJECT)

# Map: file prefix → BigQuery table
TABLES = {
    "attributes":        "bronze.cio_attributes",
    "people":            "bronze.cio_people",
    "metrics":           "bronze.cio_metrics",
    "events":            "bronze.cio_events",
    "deliveries":        "bronze.cio_deliveries",
    "delivery_contents": "bronze.cio_delivery_contents",
    "campaigns":         "bronze.cio_campaigns",
    "campaign_actions":  "bronze.cio_campaign_actions",
    "broadcasts":        "bronze.cio_broadcasts",
}


def load_table(prefix, table_name):
    """Load all parquet files matching prefix into BigQuery table."""
    gcs_uri = f"{GCS_BUCKET}/{prefix}_v5_*"
    full_table = f"{BQ_PROJECT}:{table_name}"

    print(f"  Loading {gcs_uri} → {table_name}...", end=" ")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # WRITE_TRUNCATE = replace all data each run
        # Safe because Customer.io sends full snapshots, not deltas
    )

    try:
        job = bq.load_table_from_uri(
            gcs_uri,
            f"{BQ_PROJECT}.{table_name}",
            job_config=job_config,
        )
        job.result()  # Wait for completion
        table = bq.get_table(f"{BQ_PROJECT}.{table_name}")
        print(f"✓ {table.num_rows:,} rows")
        return table.num_rows
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return 0


def main():
    print("=" * 60)
    print("CUSTOMER.IO GCS → BIGQUERY BRONZE LOADER")
    print("=" * 60)
    print(f"Source: {GCS_BUCKET}")
    print()

    total_rows = 0
    for prefix, table_name in TABLES.items():
        rows = load_table(prefix, table_name)
        total_rows += rows

    print()
    print("=" * 60)
    print(f"✅ COMPLETE — {total_rows:,} total rows loaded across {len(TABLES)} tables")
    print("=" * 60)


if __name__ == "__main__":
    main()
