"""
Datalabs DB (PostgreSQL) → BigQuery Bronze Ingestion

Reads all tables from datalabsdb PostgreSQL database
and loads into BigQuery Bronze tables.

Usage:
    export PG_HOST="datalabsdata.postgres.database.azure.com"
    export PG_USER="your_username"
    export PG_PASSWORD="your_password"
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/datalabs_ingest.py
"""

import os
import pandas as pd
import psycopg2
from google.cloud import bigquery

# ── Config ──
PG_HOST = os.environ.get("PG_HOST", "datalabsdata.postgres.database.azure.com")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_DB = os.environ.get("PG_DB", "datalabsdb")
PG_SCHEMA = "bigquery_db_datalabs_live"
PG_PORT = int(os.environ.get("PG_PORT", 5432))

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")

BQ_PROJECT = "bigquery-296406"
bq = bigquery.Client(project=BQ_PROJECT)

# All Datalabs tables → BigQuery Bronze (dl_ prefix)
TABLES = [
    "acquisition_table",
    "balance_sheet_table",
    "balance_sheet_table_ipo",
    "captable_data",
    "cashflows_table",
    "cashflows_table_ipo",
    "company_description_table",
    "company_documents",
    "company_table",
    "employee_review_rating_table",
    "employee_trendline",
    "financial_ratios_table",
    "financial_ratios_table_ipo",
    "funding_table",
    "inc42_company_tag",
    "inc42_sector_thesis",
    "investment_table",
    "investor_table",
    "jobs_listing_table",
    # "logs_table",  # skipped — 8M rows of internal logs, not needed
    "people_description_table",
    "people_education_table",
    "people_table",
    "person_contact",
    "product_review_table",
    "profit_loss_table",
    "profit_loss_table_ipo",
    "quarterly_financials_listed",
    "sector_tagging",
    "tech_stocks_table",
    "website_analytics_table",
]


def get_pg_connection():
    """Create PostgreSQL connection with SSL (required by Azure)."""
    if not PG_USER or not PG_PASSWORD:
        raise ValueError("Set PG_USER and PG_PASSWORD environment variables")

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
        sslmode="require",
    )


def load_table(pg_table):
    """Read PostgreSQL table and load into BigQuery Bronze (full refresh)."""
    bq_table = f"bronze.dl_{pg_table}"
    print(f"  {pg_table} → {bq_table}...", end=" ")

    conn = get_pg_connection()
    try:
        df = pd.read_sql(f'SELECT * FROM {PG_SCHEMA}."{pg_table}"', conn)
        print(f"({len(df):,} rows from PG)", end=" ")

        if df.empty:
            print("⚠️  Empty table, skipping")
            return 0

        # Convert datetime columns to avoid BQ type issues
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = df[col].astype(str).replace("NaT", None)

        # Convert any remaining object columns with mixed types
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if x is not None and not isinstance(x, str) else x)

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

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return 0
    finally:
        conn.close()


def main():
    print("=" * 60)
    print("DATALABS DB (POSTGRESQL) → BIGQUERY BRONZE")
    print("=" * 60)
    print(f"Source: {PG_HOST}/{PG_DB}")
    print(f"Tables: {len(TABLES)}")
    print()

    total_rows = 0
    success = 0
    failed = 0

    for pg_table in TABLES:
        rows = load_table(pg_table)
        total_rows += rows
        if rows > 0:
            success += 1
        else:
            failed += 1

    print()
    print("=" * 60)
    print(f"✅ COMPLETE — {total_rows:,} total rows loaded across {success} tables ({failed} empty/failed)")
    print("=" * 60)


if __name__ == "__main__":
    main()
