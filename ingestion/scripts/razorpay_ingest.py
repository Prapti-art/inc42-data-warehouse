"""
Razorpay (D2CX account) → BigQuery Bronze Ingestion

This Razorpay account (d2cx.co) handles D2CX summit/program payments that don't
flow through WooCommerce. Probe confirmed:
  - 100% of payments are D2CX-related (account is dedicated)
  - No course tagging in notes/description — ingest all captured payments
  - Identity join key = top-level `email` (100% fill) with `notes.first_name`,
    `notes.last_name`, `notes.phone` as enrichment

Incremental by `created_at` high-watermark (Unix epoch). First run = full history.
Idempotent via MERGE on payment `id`.

Usage:
    export RZP_KEY_ID="rzp_live_..."
    export RZP_KEY_SECRET="..."
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python3 ingestion/scripts/razorpay_ingest.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

# ── Config ──
API = "https://api.razorpay.com/v1"
PAGE_SIZE = 100
MAX_PAGES = 500
BQ_PROJECT = "bigquery-296406"
BQ_TABLE = f"{BQ_PROJECT}.bronze.razorpay_payments"

# Default for prod (container path); local dev can override via env
os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS", "/secrets/bq-service-account.json"
)

KEY_ID = os.environ.get("RZP_KEY_ID")
KEY_SECRET = os.environ.get("RZP_KEY_SECRET")
if not KEY_ID or not KEY_SECRET:
    sys.exit("ERROR: RZP_KEY_ID and RZP_KEY_SECRET must be set in env")

bq = bigquery.Client(project=BQ_PROJECT)

SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("amount_inr", "NUMERIC"),
    bigquery.SchemaField("amount_refunded_inr", "NUMERIC"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("captured", "BOOL"),
    bigquery.SchemaField("method", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("contact_phone", "STRING"),
    bigquery.SchemaField("notes_email", "STRING"),
    bigquery.SchemaField("notes_first_name", "STRING"),
    bigquery.SchemaField("notes_last_name", "STRING"),
    bigquery.SchemaField("notes_phone", "STRING"),
    bigquery.SchemaField("notes_wc_order_id", "STRING"),
    bigquery.SchemaField("card_last4", "STRING"),
    bigquery.SchemaField("card_network", "STRING"),
    bigquery.SchemaField("card_issuer", "STRING"),
    bigquery.SchemaField("vpa", "STRING"),
    bigquery.SchemaField("bank", "STRING"),
    bigquery.SchemaField("fee_inr", "NUMERIC"),
    bigquery.SchemaField("tax_inr", "NUMERIC"),
    bigquery.SchemaField("error_code", "STRING"),
    bigquery.SchemaField("error_description", "STRING"),
    bigquery.SchemaField("created_at_unix", "INT64"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("raw_payload", "JSON"),
]


def ensure_table():
    """Create bronze.razorpay_payments if it doesn't exist."""
    try:
        bq.get_table(BQ_TABLE)
        print(f"✓ Table {BQ_TABLE} exists")
    except Exception:
        table = bigquery.Table(BQ_TABLE, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="created_at"
        )
        table.clustering_fields = ["email", "status"]
        bq.create_table(table)
        print(f"✓ Created {BQ_TABLE} (day-partitioned on created_at, clustered on email/status)")


def get_high_watermark() -> int:
    """Return max(created_at_unix) from existing rows, or 0 if empty."""
    try:
        row = next(iter(bq.query(
            f"SELECT COALESCE(MAX(created_at_unix), 0) AS hwm FROM `{BQ_TABLE}`"
        ).result()))
        return int(row.hwm)
    except Exception as e:
        print(f"  (no existing data — full history pull: {e})")
        return 0


def fetch_payments(from_ts: int):
    """Page through /v1/payments since from_ts. Yields raw payment dicts."""
    skip = 0
    pages = 0
    while pages < MAX_PAGES:
        r = requests.get(
            f"{API}/payments",
            auth=(KEY_ID, KEY_SECRET),
            params={"from": from_ts, "count": PAGE_SIZE, "skip": skip},
            timeout=60,
        )
        if r.status_code != 200:
            sys.exit(f"Razorpay error {r.status_code}: {r.text[:500]}")
        items = r.json().get("items", [])
        if not items:
            break
        for p in items:
            yield p
        if len(items) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        pages += 1
        time.sleep(0.3)  # gentle rate-limit
    print(f"  fetched {skip + len(items) if items else skip} payments across {pages + 1} page(s)")


def transform(p: dict) -> dict:
    """Flatten Razorpay payment payload → bronze row matching SCHEMA."""
    notes = p.get("notes") or {}
    card = p.get("card") or {}
    created_unix = p.get("created_at") or 0
    return {
        "id": p["id"],
        "order_id": p.get("order_id"),
        "amount_inr": (p.get("amount") or 0) / 100.0,
        "amount_refunded_inr": (p.get("amount_refunded") or 0) / 100.0,
        "currency": p.get("currency"),
        "status": p.get("status"),
        "captured": bool(p.get("captured")),
        "method": p.get("method"),
        "email": (p.get("email") or "").lower() or None,
        "contact_phone": p.get("contact"),
        "notes_email": (notes.get("email") or "").lower() or None,
        "notes_first_name": notes.get("first_name"),
        "notes_last_name": notes.get("last_name"),
        "notes_phone": notes.get("phone"),
        "notes_wc_order_id": notes.get("woocommerce_order_id")
            or notes.get("woocommerce_order_number"),
        "card_last4": card.get("last4"),
        "card_network": card.get("network"),
        "card_issuer": card.get("issuer"),
        "vpa": p.get("vpa"),
        "bank": p.get("bank"),
        "fee_inr": (p.get("fee") or 0) / 100.0 if p.get("fee") else None,
        "tax_inr": (p.get("tax") or 0) / 100.0 if p.get("tax") else None,
        "error_code": p.get("error_code") or None,
        "error_description": p.get("error_description") or None,
        "created_at_unix": created_unix,
        "created_at": datetime.fromtimestamp(created_unix, tz=timezone.utc).isoformat()
            if created_unix else None,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": json.dumps(p, default=str),
    }


def merge_rows(rows: list[dict]):
    """Upsert rows into bronze table via staging + MERGE on id."""
    if not rows:
        print("  nothing to load")
        return

    staging = f"{BQ_PROJECT}.bronze.razorpay_payments__staging"
    job = bq.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ),
    )
    job.result()

    merge_sql = f"""
    MERGE `{BQ_TABLE}` T
    USING `{staging}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      order_id = S.order_id, amount_inr = S.amount_inr,
      amount_refunded_inr = S.amount_refunded_inr, currency = S.currency,
      status = S.status, captured = S.captured, method = S.method,
      email = S.email, contact_phone = S.contact_phone,
      notes_email = S.notes_email, notes_first_name = S.notes_first_name,
      notes_last_name = S.notes_last_name, notes_phone = S.notes_phone,
      notes_wc_order_id = S.notes_wc_order_id,
      card_last4 = S.card_last4, card_network = S.card_network,
      card_issuer = S.card_issuer, vpa = S.vpa, bank = S.bank,
      fee_inr = S.fee_inr, tax_inr = S.tax_inr,
      error_code = S.error_code, error_description = S.error_description,
      created_at_unix = S.created_at_unix, created_at = S.created_at,
      ingested_at = S.ingested_at, raw_payload = S.raw_payload
    WHEN NOT MATCHED THEN INSERT ROW
    """
    bq.query(merge_sql).result()
    bq.delete_table(staging, not_found_ok=True)
    print(f"✓ Merged {len(rows)} rows into {BQ_TABLE}")


def main():
    print(f"Razorpay → {BQ_TABLE}")
    print(f"  account mode: {'LIVE' if KEY_ID.startswith('rzp_live_') else 'TEST'}")
    ensure_table()

    EPOCH_FLOOR = 946684800  # 2000-01-01 — Razorpay's minimum allowed `from`
    hwm = get_high_watermark()
    if hwm == 0:
        from_ts = EPOCH_FLOOR
        print(f"  high-watermark: 0 (first run — pulling full history from 2000-01-01)")
    else:
        from_ts = max(hwm, EPOCH_FLOOR)
        print(f"  high-watermark: {hwm} ({datetime.fromtimestamp(hwm, tz=timezone.utc).isoformat()})")

    rows = [transform(p) for p in fetch_payments(from_ts)]
    print(f"  transformed {len(rows)} rows")
    merge_rows(rows)


if __name__ == "__main__":
    main()
