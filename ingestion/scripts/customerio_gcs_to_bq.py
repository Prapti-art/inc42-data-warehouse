"""
Customer.io GCS → BigQuery Bronze Loader (multi-workspace).

Customer.io drops parquet files into GCS every 10 minutes (one path per
workspace). This loader iterates over every workspace defined in WORKSPACES
and loads each into a namespaced set of bronze tables.

Run daily via Airflow. WRITE_TRUNCATE per table since Customer.io sends
cumulative snapshots, not incremental deltas.

Add a new workspace: append to WORKSPACES list, redeploy. No code changes.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python3 ingestion/scripts/customerio_gcs_to_bq.py [--workspace main]
"""
import argparse
import os
import sys

from google.cloud import bigquery

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json",
)
BQ_PROJECT = "bigquery-296406"

# Add a workspace by appending here. `bronze_prefix` namespaces the tables so
# multiple workspaces never collide.
WORKSPACES = [
    {
        "name": "main",
        "workspace_id": 208301,
        "gcs_path": "gs://inc42-data-warehouse-raw/customerio",
        "bronze_prefix": "cio_",
    },
    {
        "name": "datalabs",
        "workspace_id": 208719,
        "gcs_path": "gs://inc42-data-warehouse-raw/datalabs_customerio",
        "bronze_prefix": "cio_datalabs_",
    },
]

# All parquet types CIO can export. Workspaces with no data for a given type
# are skipped silently — the loader simply finds no matching files.
FILE_TYPES = [
    "attributes",
    "people",
    "metrics",
    "events",
    "deliveries",
    "delivery_contents",
    "campaigns",
    "campaign_actions",
    "broadcasts",
    # CIO Copilot / newer-feature outputs (present in Datalabs workspace)
    "outputs",
    "subjects",
]


def load_one(bq, gcs_path, prefix, table):
    """Load all parquet files matching <gcs_path>/<prefix>_v5_* into <table>."""
    uri = f"{gcs_path}/{prefix}_v5_*"
    full_table = f"{BQ_PROJECT}.{table}"
    print(f"    {prefix:18s} → {table:35s}", end=" ")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    try:
        job = bq.load_table_from_uri(uri, full_table, job_config=job_config)
        job.result()
        n = bq.get_table(full_table).num_rows
        print(f"✓ {n:,} rows")
        return n
    except Exception as e:
        msg = str(e)
        # Empty match (no files) — silently skip; not an error for workspaces
        # that don't export that file type.
        if "Not found" in msg or "matched no files" in msg or "No matching files" in msg.lower():
            print("· (no files)")
            return 0
        print(f"✗ ERROR: {msg[:200]}")
        return 0


def load_workspace(bq, ws):
    print(f"\n  Workspace: {ws['name']} (id={ws['workspace_id']})")
    print(f"  Source:    {ws['gcs_path']}")
    total = 0
    for ft in FILE_TYPES:
        bronze_table = f"bronze.{ws['bronze_prefix']}{ft}"
        total += load_one(bq, ws["gcs_path"], ft, bronze_table)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--workspace",
        choices=[w["name"] for w in WORKSPACES] + ["all"],
        default="all",
        help="Workspace to load (default: all)",
    )
    args = ap.parse_args()

    print("=" * 60)
    print("CUSTOMER.IO → BIGQUERY BRONZE LOADER")
    print("=" * 60)

    bq = bigquery.Client(project=BQ_PROJECT)
    targets = WORKSPACES if args.workspace == "all" else [
        w for w in WORKSPACES if w["name"] == args.workspace
    ]
    grand_total = 0
    for ws in targets:
        grand_total += load_workspace(bq, ws)

    print(f"\n{'=' * 60}")
    print(f"✅ COMPLETE — {grand_total:,} rows across {len(targets)} workspace(s)")
    print("=" * 60)


if __name__ == "__main__":
    main()
