"""
HubSpot CRM v3 → BigQuery Bronze Ingestion

Fetches contacts, companies, and deals from HubSpot and loads them into
bronze.hubspot_contacts / bronze.hubspot_companies / bronze.hubspot_deals.

Incremental: uses `hs_lastmodifieddate` filter via the CRM v3 search API,
with a cursor stored per-object in bronze._ingest_state.

Usage:
    export HUBSPOT_PRIVATE_APP_TOKEN="pat-eu1-..."
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/hubspot_ingest.py

Auth: Create a Private App in HubSpot (Settings → Integrations → Private Apps)
      with scopes: crm.objects.{contacts,companies,deals}.read and the
      corresponding crm.schemas.*.read scopes.

API Docs: https://developers.hubspot.com/docs/api/crm/contacts
Rate Limit: 100 requests per 10 seconds (private app, free/starter tier).
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

HS_TOKEN = os.environ.get("HUBSPOT_PRIVATE_APP_TOKEN")
if not HS_TOKEN:
    raise ValueError(
        "Set HUBSPOT_PRIVATE_APP_TOKEN. Create at: "
        "https://app.hubspot.com/private-apps"
    )

BQ_PROJECT = "bigquery-296406"
BQ_DATASET = "bronze"
STATE_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}._ingest_state"
HS_BASE = "https://api.hubapi.com"

HEADERS = {
    "Authorization": f"Bearer {HS_TOKEN}",
    "Content-Type": "application/json",
}

# Stay under 100 req / 10s → ~8 RPS with safety margin.
THROTTLE_SECONDS = 0.12

# Initial backfill window if no cursor exists yet.
INITIAL_LOOKBACK_DAYS = int(os.environ.get("HUBSPOT_INITIAL_LOOKBACK_DAYS", "3650"))

bq = bigquery.Client(project=BQ_PROJECT)


# ══════════════════════════════════════════════════════════════
# Object configs — properties to pull, flat columns to extract
# ══════════════════════════════════════════════════════════════
OBJECTS = {
    "contacts": {
        "table": f"{BQ_PROJECT}.{BQ_DATASET}.hubspot_contacts",
        "endpoint": "/crm/v3/objects/contacts/search",
        "properties": [
            "email", "firstname", "lastname", "phone", "mobilephone",
            "jobtitle", "company", "industry", "city", "state", "country",
            "lifecyclestage", "hs_lead_status", "lead_source",
            "createdate", "lastmodifieddate", "hs_lastmodifieddate",
            "hs_object_id", "hubspot_owner_id",
            "linkedin_url", "hs_analytics_source",
            "unified_contact_id",
        ],
        "flat_columns": [
            ("hs_object_id", "id", "STRING"),
            ("email", "email", "STRING"),
            ("firstname", "first_name", "STRING"),
            ("lastname", "last_name", "STRING"),
            ("phone", "phone", "STRING"),
            ("mobilephone", "mobile_phone", "STRING"),
            ("jobtitle", "job_title", "STRING"),
            ("company", "company_name", "STRING"),
            ("industry", "industry", "STRING"),
            ("city", "city", "STRING"),
            ("state", "state", "STRING"),
            ("country", "country", "STRING"),
            ("lifecyclestage", "lifecycle_stage", "STRING"),
            ("hs_lead_status", "lead_status", "STRING"),
            ("lead_source", "lead_source", "STRING"),
            ("linkedin_url", "linkedin_url", "STRING"),
            ("hubspot_owner_id", "owner_id", "STRING"),
            ("unified_contact_id", "unified_contact_id", "STRING"),
            ("createdate", "created_at", "TIMESTAMP"),
            ("hs_lastmodifieddate", "modified_at", "TIMESTAMP"),
        ],
    },
    "companies": {
        "table": f"{BQ_PROJECT}.{BQ_DATASET}.hubspot_companies",
        "endpoint": "/crm/v3/objects/companies/search",
        "properties": [
            "name", "domain", "industry", "numberofemployees", "annualrevenue",
            "city", "state", "country", "founded_year",
            "lifecyclestage", "type", "description",
            "createdate", "hs_lastmodifieddate", "hs_object_id",
            "hubspot_owner_id",
        ],
        "flat_columns": [
            ("hs_object_id", "id", "STRING"),
            ("name", "name", "STRING"),
            ("domain", "domain", "STRING"),
            ("industry", "industry", "STRING"),
            ("numberofemployees", "employee_count", "STRING"),
            ("annualrevenue", "annual_revenue", "STRING"),
            ("city", "city", "STRING"),
            ("state", "state", "STRING"),
            ("country", "country", "STRING"),
            ("founded_year", "founded_year", "STRING"),
            ("lifecyclestage", "lifecycle_stage", "STRING"),
            ("type", "type", "STRING"),
            ("hubspot_owner_id", "owner_id", "STRING"),
            ("createdate", "created_at", "TIMESTAMP"),
            ("hs_lastmodifieddate", "modified_at", "TIMESTAMP"),
        ],
    },
    "deals": {
        "table": f"{BQ_PROJECT}.{BQ_DATASET}.hubspot_deals",
        "endpoint": "/crm/v3/objects/deals/search",
        "properties": [
            "dealname", "amount", "pipeline", "dealstage", "closedate",
            "createdate", "hs_lastmodifieddate", "hs_object_id",
            "hubspot_owner_id", "dealtype", "hs_is_closed", "hs_is_closed_won",
        ],
        "flat_columns": [
            ("hs_object_id", "id", "STRING"),
            ("dealname", "deal_name", "STRING"),
            ("amount", "amount", "STRING"),
            ("pipeline", "pipeline", "STRING"),
            ("dealstage", "deal_stage", "STRING"),
            ("dealtype", "deal_type", "STRING"),
            ("hs_is_closed", "is_closed", "STRING"),
            ("hs_is_closed_won", "is_closed_won", "STRING"),
            ("hubspot_owner_id", "owner_id", "STRING"),
            ("closedate", "close_date", "TIMESTAMP"),
            ("createdate", "created_at", "TIMESTAMP"),
            ("hs_lastmodifieddate", "modified_at", "TIMESTAMP"),
        ],
    },
}


# ══════════════════════════════════════════════════════════════
# State: cursor per object (hs_lastmodifieddate of most recent row)
# ══════════════════════════════════════════════════════════════
def ensure_state_table():
    schema = [
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("object", "STRING"),
        bigquery.SchemaField("last_modified_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(STATE_TABLE, schema=schema)
    try:
        bq.get_table(STATE_TABLE)
    except Exception:
        bq.create_table(table)
        print(f"  ✓ Created state table {STATE_TABLE}")


def get_cursor(object_name):
    q = f"""
        SELECT last_modified_at
        FROM `{STATE_TABLE}`
        WHERE source = 'hubspot' AND object = @object
        ORDER BY updated_at DESC LIMIT 1
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("object", "STRING", object_name)]
    ))
    rows = list(job.result())
    if rows and rows[0].last_modified_at:
        return rows[0].last_modified_at
    return datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)


def set_cursor(object_name, last_modified_at):
    if not last_modified_at:
        return
    row = {
        "source": "hubspot",
        "object": object_name,
        "last_modified_at": last_modified_at.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = bq.insert_rows_json(STATE_TABLE, [row])
    if errors:
        print(f"  ⚠️  Failed to write cursor: {errors}")


# ══════════════════════════════════════════════════════════════
# Bronze table setup
# ══════════════════════════════════════════════════════════════
def ensure_bronze_table(object_name, cfg):
    fixed = [
        bigquery.SchemaField(col_name, col_type)
        for _, col_name, col_type in cfg["flat_columns"]
    ]
    fixed += [
        bigquery.SchemaField("properties_json", "STRING"),
        bigquery.SchemaField("associations_json", "STRING"),
        bigquery.SchemaField("archived", "BOOLEAN"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
        bigquery.SchemaField("_ingestion_id", "STRING"),
    ]
    table = bigquery.Table(cfg["table"], schema=fixed)
    table.time_partitioning = bigquery.TimePartitioning(
        field="_ingested_at", type_=bigquery.TimePartitioningType.DAY
    )
    try:
        bq.get_table(cfg["table"])
        print(f"  ✓ Table {cfg['table']} exists")
    except Exception:
        bq.create_table(table)
        print(f"  ✓ Created {cfg['table']}")


# ══════════════════════════════════════════════════════════════
# Fetch (paginated search with hs_lastmodifieddate filter)
# ══════════════════════════════════════════════════════════════
def fetch_objects(object_name, cfg, since_dt):
    since_ms = int(since_dt.timestamp() * 1000)
    print(f"  📥 Fetching {object_name} modified since {since_dt.isoformat()}")

    results = []
    after = None
    page = 0

    while True:
        page += 1
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GTE",
                    "value": str(since_ms),
                }]
            }],
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
            "properties": cfg["properties"],
            "limit": 100,
        }
        if after:
            body["after"] = after

        resp = requests.post(f"{HS_BASE}{cfg['endpoint']}", headers=HEADERS, json=body)

        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", "10"))
            print(f"    ⏳ Rate limited, sleeping {retry}s")
            time.sleep(retry)
            continue

        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        results.extend(batch)
        print(f"    page {page}: +{len(batch)} (total {len(results)})")

        paging = data.get("paging", {}).get("next")
        if not paging:
            break
        after = paging.get("after")
        if not after:
            break

        time.sleep(THROTTLE_SECONDS)

    print(f"  ✓ Fetched {len(results)} {object_name}")
    return results


# ══════════════════════════════════════════════════════════════
# Flatten + load
# ══════════════════════════════════════════════════════════════
def parse_hs_timestamp(value):
    """HubSpot returns either ISO 8601 strings or epoch ms."""
    if value is None or value == "":
        return None
    if isinstance(value, str) and "T" in value:
        return value
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def flatten(obj, cfg, ingestion_id):
    props = obj.get("properties", {}) or {}
    row = {}

    for hs_key, col_name, col_type in cfg["flat_columns"]:
        raw = props.get(hs_key)
        if col_type == "TIMESTAMP":
            row[col_name] = parse_hs_timestamp(raw)
        else:
            row[col_name] = str(raw) if raw is not None else None

    row["properties_json"] = json.dumps(props)
    row["associations_json"] = json.dumps(obj.get("associations", {}))
    row["archived"] = bool(obj.get("archived", False))
    row["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    row["_source"] = "hubspot_api"
    row["_ingestion_id"] = ingestion_id
    return row


def load_bronze(object_name, cfg, rows):
    if not rows:
        print(f"  ⏭️  Nothing to load for {object_name}")
        return None

    import pandas as pd
    df = pd.DataFrame(rows)
    for _, col_name, col_type in cfg["flat_columns"]:
        if col_type == "TIMESTAMP":
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce", utc=True)
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce", utc=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )
    job = bq.load_table_from_dataframe(df, cfg["table"], job_config=job_config)
    job.result()
    print(f"  ✓ Loaded {len(rows)} rows into {cfg['table']}")

    valid = df["modified_at"].dropna()
    return valid.max().to_pydatetime() if not valid.empty else None


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("HUBSPOT CRM v3 → BIGQUERY BRONZE INGESTION")
    print("=" * 60)

    ensure_state_table()
    ingestion_id = datetime.now(timezone.utc).strftime("hubspot_%Y%m%dT%H%M%SZ")

    summary = {}

    for object_name, cfg in OBJECTS.items():
        print(f"\n▶ {object_name.upper()}")
        ensure_bronze_table(object_name, cfg)

        since_dt = get_cursor(object_name)
        raw = fetch_objects(object_name, cfg, since_dt)
        rows = [flatten(o, cfg, ingestion_id) for o in raw]

        new_cursor = load_bronze(object_name, cfg, rows)
        if new_cursor:
            set_cursor(object_name, new_cursor)

        summary[object_name] = len(rows)

    print("\n" + "=" * 60)
    print("✅ HUBSPOT INGESTION COMPLETE")
    print("=" * 60)
    for name, count in summary.items():
        print(f"  {name:12s}: {count:>6} rows")
    print(f"  ingestion_id: {ingestion_id}")


if __name__ == "__main__":
    main()
