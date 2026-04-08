"""
Gravity Forms (MySQL) → BigQuery Bronze Ingestion

Reads 42_gf_entry and 42_gf_entry_meta from inc42prod MySQL database
and loads into BigQuery Bronze tables.

Usage:
    export MYSQL_HOST="inc42-prod.mysql.database.azure.com"
    export MYSQL_USER="your_username"
    export MYSQL_PASSWORD="your_password"
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/gravity_forms_ingest.py
"""

import os
import pandas as pd
import mysql.connector
from google.cloud import bigquery

# ── Config ──
MYSQL_HOST = os.environ.get("MYSQL_HOST", "inc42-prod.mysql.database.azure.com")
MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DB = "inc42prod"
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")

BQ_PROJECT = "bigquery-296406"
bq = bigquery.Client(project=BQ_PROJECT)

# Tables to sync
TABLES = {
    "42_gf_entry": "bronze.gravity_forms_entries",
    "42_gf_entry_meta": "bronze.gravity_forms_entry_meta",
}


def get_mysql_connection():
    """Create MySQL connection with SSL (required by Azure)."""
    if not MYSQL_USER or not MYSQL_PASSWORD:
        raise ValueError("Set MYSQL_USER and MYSQL_PASSWORD environment variables")

    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        ssl_disabled=False,
    )


def load_table(mysql_table, bq_table):
    """Read MySQL table and load into BigQuery Bronze (full refresh)."""
    print(f"  {mysql_table} → {bq_table}...", end=" ")

    conn = get_mysql_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM `{mysql_table}`", conn)
        print(f"({len(df):,} rows from MySQL)", end=" ")

        if df.empty:
            print("⚠️  Empty table, skipping")
            return 0

        # Convert datetime columns to avoid BQ type issues
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str).replace("NaT", None)

        # Load to BigQuery (WRITE_TRUNCATE = full refresh)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        full_table = f"{BQ_PROJECT}.{bq_table}"
        job = bq.load_table_from_dataframe(df, full_table, job_config=job_config)
        job.result()

        table = bq.get_table(full_table)
        print(f"✓ {table.num_rows:,} rows in BQ")
        return table.num_rows

    finally:
        conn.close()


def main():
    print("=" * 60)
    print("GRAVITY FORMS (MYSQL) → BIGQUERY BRONZE")
    print("=" * 60)
    print(f"Source: {MYSQL_HOST}/{MYSQL_DB}")
    print()

    total_rows = 0
    for mysql_table, bq_table in TABLES.items():
        rows = load_table(mysql_table, bq_table)
        total_rows += rows

    print()
    print("=" * 60)
    print(f"✅ COMPLETE — {total_rows:,} total rows loaded across {len(TABLES)} tables")
    print("=" * 60)


if __name__ == "__main__":
    main()
