"""
Inc42 DB (MySQL) → BigQuery Bronze Ingestion

Reads 42_users and 42_usermeta from inc42prod MySQL database
and loads into BigQuery Bronze tables.

Usage:
    export MYSQL_HOST="inc42-prod.mysql.database.azure.com"
    export MYSQL_USER="your_username"
    export MYSQL_PASSWORD="your_password"
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/inc42_db_ingest.py
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

# Tables to sync: MySQL table → BigQuery Bronze table
TABLES = {
    "42_users": "bronze.inc42_users",
    "42_usermeta": "bronze.inc42_usermeta",
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


CHUNK_SIZE = 100_000


def load_table(mysql_table, bq_table):
    """Read MySQL table and stream into BigQuery Bronze (full refresh).

    Streams in chunks of CHUNK_SIZE rows: first chunk WRITE_TRUNCATE
    (replaces the table + resets schema), rest WRITE_APPEND. Keeps
    pandas memory bounded so we don't OOM the 4GB container on the
    larger MySQL tables (inc42_usermeta in particular).
    """
    print(f"  {mysql_table} → {bq_table}...", end=" ", flush=True)

    full_table = f"{BQ_PROJECT}.{bq_table}"
    conn = get_mysql_connection()
    total_rows = 0
    first_chunk = True
    try:
        for chunk in pd.read_sql(
            f"SELECT * FROM `{mysql_table}`",
            conn,
            chunksize=CHUNK_SIZE,
        ):
            if chunk.empty:
                continue
            job_config = bigquery.LoadJobConfig(
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE if first_chunk
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
                autodetect=True,
            )
            job = bq.load_table_from_dataframe(chunk, full_table, job_config=job_config)
            job.result()
            total_rows += len(chunk)
            first_chunk = False
            print(f"[{total_rows:,}]", end=" ", flush=True)

        if total_rows == 0:
            print("⚠️  Empty table, skipping")
            return 0

        table = bq.get_table(full_table)
        print(f"✓ {table.num_rows:,} rows in BQ")
        return table.num_rows

    finally:
        conn.close()


def main():
    print("=" * 60)
    print("INC42 DB (MYSQL) → BIGQUERY BRONZE")
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
