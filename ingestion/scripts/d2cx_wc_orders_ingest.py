"""
D2CX WooCommerce orders (CSV-based) → BigQuery Bronze

Loads order exports from d2cx.co's 3 WooCommerce stores:
  - D2CX Foundations
  - D2CX AI
  - D2CX Applications

Each store has its own WC install; order IDs overlap across stores → we
disambiguate via (source_store, order_id) MERGE key.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python3 ingestion/scripts/d2cx_wc_orders_ingest.py [--data-dir /path/to/csvs]

Re-running is idempotent: MERGE on (source_store, order_id).
"""
import argparse
import csv
import io
import os
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/bq-service-account.json")
BQ_PROJECT = "bigquery-296406"
BQ_TABLE = f"{BQ_PROJECT}.bronze.d2cx_wc_orders"

# Map filename → source_store tag. Matches what's currently in Downloads/.
# In prod the CSVs would live at a fixed path (GCS or container mount).
FILE_MAP = {
    "Orders-Export-2026-June-09-0850.csv": "d2cx_foundations",
    "Orders-Export-2026-June-09-0850 (1).csv": "d2cx_ai",
    "Orders-Export-2026-June-09-0850 (2).csv": "d2cx_applications",
}

# Subset of WC export columns we actually keep.
COLS = [
    ("Order ID", "order_id", "STRING"),
    ("Order Date", "order_date", "TIMESTAMP"),
    ("Order Status", "order_status", "STRING"),
    ("Order Total", "order_total_inr", "FLOAT64"),
    ("Date Paid", "date_paid", "TIMESTAMP"),
    ("Billing Email Address", "billing_email", "STRING"),
    ("Billing First Name", "billing_first_name", "STRING"),
    ("Billing Last Name", "billing_last_name", "STRING"),
    ("Billing Phone", "billing_phone", "STRING"),
    ("Billing Company", "billing_company", "STRING"),
    ("Billing City", "billing_city", "STRING"),
    ("Billing State", "billing_state", "STRING"),
    ("Billing Country", "billing_country", "STRING"),
    ("Product ID", "product_id", "STRING"),
    ("SKU", "sku", "STRING"),
    ("Product Name", "product_name", "STRING"),
    ("Quantity", "quantity", "INT64"),
    ("Item Total", "item_total_inr", "FLOAT64"),
    ("Coupon Code", "coupon_code", "STRING"),
]

SCHEMA = [
    bigquery.SchemaField("source_store", "STRING", mode="REQUIRED"),
    *[bigquery.SchemaField(out, t) for _, out, t in COLS],
    bigquery.SchemaField("event_edition_from_date", "STRING"),  # fallback edition = year(order_date)
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]


def parse_csv(path: str, source_store: str):
    """Read one WC export. Strips NUL bytes (file 1 has some)."""
    with open(path, "rb") as f:
        raw = f.read().replace(b"\x00", b"")
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    hdr = next(reader)
    col_idx = {src: hdr.index(src) for src, _, _ in COLS if src in hdr}
    missing = [src for src, _, _ in COLS if src not in col_idx]
    if missing:
        print(f"  ⚠ missing cols in {os.path.basename(path)}: {missing}")

    rows = []
    for row in reader:
        if not row or len(row) < max(col_idx.values(), default=0) + 1:
            continue
        out = {"source_store": source_store, "ingested_at": datetime.now(timezone.utc).isoformat()}
        for src, dst, typ in COLS:
            if src not in col_idx:
                out[dst] = None
                continue
            v = row[col_idx[src]].strip()
            if v == "":
                out[dst] = None
            elif typ in ("FLOAT64",):
                try: out[dst] = float(v)
                except: out[dst] = None
            elif typ == "INT64":
                try: out[dst] = int(float(v))
                except: out[dst] = None
            elif typ == "TIMESTAMP":
                # WC uses "YYYY-MM-DD HH:MM:SS" — convert to ISO
                try:
                    out[dst] = datetime.strptime(v, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
                except: out[dst] = None
            else:
                out[dst] = v
        # Fallback edition = YYYY from order_date
        if out.get("order_date"):
            out["event_edition_from_date"] = out["order_date"][:4]
        else:
            out["event_edition_from_date"] = None
        # Normalize email to lowercase for join consistency
        if out.get("billing_email"):
            out["billing_email"] = out["billing_email"].lower()
        rows.append(out)
    return rows


def merge_into_bronze(bq, all_rows):
    if not all_rows:
        print("nothing to load")
        return
    staging = f"{BQ_PROJECT}.bronze.d2cx_wc_orders__staging"
    job = bq.load_table_from_json(
        all_rows, staging,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ),
    )
    job.result()

    # MERGE on (source_store, order_id) — line-item granularity preserved by also dedup
    # within staging via product_id (one order can have multiple line items).
    set_cols = [out for _, out, _ in COLS if out != "order_id"] + [
        "source_store", "event_edition_from_date", "ingested_at"
    ]
    set_clause = ", ".join(f"{c} = S.{c}" for c in set_cols if c != "source_store")
    merge_sql = f"""
    MERGE `{BQ_TABLE}` T
    USING `{staging}` S
    ON  T.source_store = S.source_store
    AND T.order_id     = S.order_id
    AND COALESCE(T.product_id,'') = COALESCE(S.product_id,'')
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ROW
    """
    bq.query(merge_sql).result()
    bq.delete_table(staging, not_found_ok=True)
    print(f"✓ Merged {len(all_rows)} rows into {BQ_TABLE}")


def ensure_table(bq):
    try:
        bq.get_table(BQ_TABLE)
        print(f"✓ {BQ_TABLE} exists")
    except Exception:
        t = bigquery.Table(BQ_TABLE, schema=SCHEMA)
        t.clustering_fields = ["source_store", "billing_email"]
        bq.create_table(t)
        print(f"✓ Created {BQ_TABLE} (clustered on source_store, billing_email)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="/Users/inc42/Downloads",
                   help="Directory holding the 3 D2CX CSV exports")
    args = p.parse_args()

    bq = bigquery.Client(project=BQ_PROJECT)
    ensure_table(bq)

    all_rows = []
    for fname, store in FILE_MAP.items():
        path = os.path.join(args.data_dir, fname)
        if not os.path.exists(path):
            sys.exit(f"ERROR: missing CSV {path}")
        rows = parse_csv(path, store)
        print(f"  {store}: parsed {len(rows)} line items from {fname}")
        all_rows.extend(rows)

    merge_into_bronze(bq, all_rows)


if __name__ == "__main__":
    main()
