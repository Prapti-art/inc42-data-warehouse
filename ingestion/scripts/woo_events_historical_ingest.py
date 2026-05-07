#!/usr/bin/env python3
"""One-time ingestion of historical WooCommerce event-ticket exports into BigQuery.

Loads WooCommerce CSV exports (D2C Summit 2.0/2023/3.0, GenAI Summit 2024/2025,
Fintech Summit, Startup Leaders Pass) into bronze.woo_events_historical so they
can flow through fact_orders -> contact_360.

Auth: set GOOGLE_APPLICATION_CREDENTIALS to the service account JSON path.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
    python woo_events_historical_ingest.py path/to/export1.csv path/to/export2.csv

Or set CSV paths via env var (colon-separated):
    export WOO_EVENTS_CSV_PATHS=/path/a.csv:/path/b.csv
    python woo_events_historical_ingest.py

Project/dataset overridable via env: BQ_PROJECT, BQ_BRONZE_DATASET.

This is a one-time backfill. Re-running TRUNCATEs and reloads (idempotent).
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

csv.field_size_limit(sys.maxsize)

PROJECT = os.getenv("BQ_PROJECT", "bigquery-296406")
DATASET = os.getenv("BQ_BRONZE_DATASET", "bronze")
TABLE = "woo_events_historical"
TABLE_FQN = f"{PROJECT}.{DATASET}.{TABLE}"

# Mirror silver/contacts.sql junk-email filter
EMAIL_JUNK = re.compile(r"(test|testing|example|sample|mailinator)", re.IGNORECASE)
EMAIL_VALID = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", re.IGNORECASE)


def read_csv_stripping_nuls(path: Path):
    """File 1 contains NUL bytes; strip them before parsing."""
    with open(path, "rb") as f:
        text = f.read().replace(b"\x00", b"").decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    return header, list(reader)


def g(row, idx, col, default=""):
    i = idx.get(col)
    if i is None or i >= len(row):
        return default
    return (row[i] or "").strip()


def to_decimal(s):
    if not s:
        return None
    try:
        return float(str(s).replace(",", ""))
    except (ValueError, AttributeError):
        return None


def to_int(s):
    if not s:
        return None
    try:
        return int(float(s))
    except (ValueError, AttributeError):
        return None


def to_ts(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def first_nonempty(row, idx, *cols):
    for c in cols:
        v = g(row, idx, c)
        if v:
            return v
    return None


def refund_amount(row, idx):
    """Combination logic: prefer line-item Refund Total; fall back to order_total
    when order_status='wc-refunded'. Returns 0.0 for non-refunds."""
    rt = to_decimal(g(row, idx, "Refund Total"))
    if rt and rt > 0:
        return rt
    if g(row, idx, "Order Status") == "wc-refunded":
        return to_decimal(g(row, idx, "Order Total")) or 0.0
    return 0.0


def normalize_row(row, idx, source_file):
    email = g(row, idx, "Billing Email Address").lower()
    if not email or EMAIL_JUNK.search(email) or not EMAIL_VALID.match(email):
        return None

    designation = first_nonempty(
        row, idx, "_billing_designation", "billing_designation"
    )
    if designation and designation.strip().lower() == "test":
        return None  # explicit test row

    return {
        "order_id": to_int(g(row, idx, "Order ID")),
        "order_key": g(row, idx, "Order Key") or None,
        "order_date": to_ts(g(row, idx, "Order Date")),
        "completed_date": to_ts(g(row, idx, "Completed Date")),
        "refund_date": to_ts(g(row, idx, "Refund Date")),
        "order_status": g(row, idx, "Order Status") or None,
        "currency": g(row, idx, "Order Currency") or None,
        "payment_method": g(row, idx, "Payment Method Title") or None,
        "order_total": to_decimal(g(row, idx, "Order Total")),
        "customer_user_id": to_int(g(row, idx, "Customer User ID")),
        "billing_email": email,
        "billing_first_name": g(row, idx, "Billing First Name") or None,
        "billing_last_name": g(row, idx, "Billing Last Name") or None,
        "billing_company": first_nonempty(
            row, idx, "Billing Company", "_billing_company_name",
            "billing_company_name", "company_name_1",
        ),
        "billing_phone": g(row, idx, "Billing Phone") or None,
        "billing_city": g(row, idx, "Billing City") or None,
        "billing_state": g(row, idx, "Billing State") or None,
        "billing_country": g(row, idx, "Billing Country") or None,
        "billing_postcode": g(row, idx, "Billing Postcode") or None,
        "billing_designation": designation,
        "billing_seniority": first_nonempty(
            row, idx, "_billing_seniority", "billing_seniority",
        ),
        "billing_linkedin": first_nonempty(
            row, idx, "_billing_linkedin", "billing_linkedin",
            "_billing_social_url", "billing_social_url",
        ),
        "billing_industry": first_nonempty(
            row, idx, "_billing_primary_industry", "billing_primary_industry",
        ),
        "product_id": to_int(g(row, idx, "Product ID")),
        "sku": g(row, idx, "SKU") or None,
        "product_name": g(row, idx, "Product Name") or None,
        "pass_type": g(row, idx, "Product Variation Details (pass-type)") or None,
        "quantity": to_int(g(row, idx, "Quantity")),
        "item_cost": to_decimal(g(row, idx, "Item Cost")),
        "item_total": to_decimal(g(row, idx, "Item Total")),
        "item_tax": to_decimal(g(row, idx, "Item Tax")),
        "refund_total": refund_amount(row, idx),
        "refund_reason": g(row, idx, "Refund Reason") or None,
        "coupon_code": g(row, idx, "Coupon Code") or None,
        "discount_amount": to_decimal(g(row, idx, "Discount Amount")),
        "source_file": source_file,
        "source_system": "woo_events_historical",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


SCHEMA = [
    bigquery.SchemaField("order_id", "INT64"),
    bigquery.SchemaField("order_key", "STRING"),
    bigquery.SchemaField("order_date", "TIMESTAMP"),
    bigquery.SchemaField("completed_date", "TIMESTAMP"),
    bigquery.SchemaField("refund_date", "TIMESTAMP"),
    bigquery.SchemaField("order_status", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("payment_method", "STRING"),
    bigquery.SchemaField("order_total", "NUMERIC"),
    bigquery.SchemaField("customer_user_id", "INT64"),
    bigquery.SchemaField("billing_email", "STRING"),
    bigquery.SchemaField("billing_first_name", "STRING"),
    bigquery.SchemaField("billing_last_name", "STRING"),
    bigquery.SchemaField("billing_company", "STRING"),
    bigquery.SchemaField("billing_phone", "STRING"),
    bigquery.SchemaField("billing_city", "STRING"),
    bigquery.SchemaField("billing_state", "STRING"),
    bigquery.SchemaField("billing_country", "STRING"),
    bigquery.SchemaField("billing_postcode", "STRING"),
    bigquery.SchemaField("billing_designation", "STRING"),
    bigquery.SchemaField("billing_seniority", "STRING"),
    bigquery.SchemaField("billing_linkedin", "STRING"),
    bigquery.SchemaField("billing_industry", "STRING"),
    bigquery.SchemaField("product_id", "INT64"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("pass_type", "STRING"),
    bigquery.SchemaField("quantity", "INT64"),
    bigquery.SchemaField("item_cost", "NUMERIC"),
    bigquery.SchemaField("item_total", "NUMERIC"),
    bigquery.SchemaField("item_tax", "NUMERIC"),
    bigquery.SchemaField("refund_total", "NUMERIC"),
    bigquery.SchemaField("refund_reason", "STRING"),
    bigquery.SchemaField("coupon_code", "STRING"),
    bigquery.SchemaField("discount_amount", "NUMERIC"),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("source_system", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]


def resolve_csv_paths(argv_paths):
    if argv_paths:
        return [Path(p) for p in argv_paths]
    env_paths = os.getenv("WOO_EVENTS_CSV_PATHS")
    if env_paths:
        return [Path(p) for p in env_paths.split(":") if p.strip()]
    sys.exit(
        "Provide CSV paths as positional args, or set WOO_EVENTS_CSV_PATHS "
        "(colon-separated) — e.g. /path/a.csv:/path/b.csv"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_paths", nargs="*", help="WooCommerce export CSV paths")
    args = parser.parse_args()

    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        sys.exit(
            "Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path"
        )

    csv_files = resolve_csv_paths(args.csv_paths)
    for p in csv_files:
        if not p.exists():
            sys.exit(f"File not found: {p}")

    print(f"Loading into {TABLE_FQN}\n")
    all_rows = []
    for path in csv_files:
        header, csv_rows = read_csv_stripping_nuls(path)
        idx = {h: i for i, h in enumerate(header)}
        kept = 0
        for r in csv_rows:
            n = normalize_row(r, idx, path.name)
            if n is not None:
                all_rows.append(n)
                kept += 1
        print(f"  {path.name}: {len(csv_rows)} read, {kept} kept "
              f"({len(csv_rows) - kept} dropped — junk emails / test rows)")

    print(f"\nTotal rows to load: {len(all_rows)}")

    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    ndjson = ("\n".join(json.dumps(r, default=str) for r in all_rows)).encode("utf-8")
    job = client.load_table_from_file(
        io.BytesIO(ndjson), TABLE_FQN, job_config=job_config
    )
    job.result()
    table = client.get_table(TABLE_FQN)
    print(f"Loaded {table.num_rows} rows into {TABLE_FQN}")


if __name__ == "__main__":
    main()
