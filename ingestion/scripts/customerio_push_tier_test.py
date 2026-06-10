"""
Customer.io Reverse ETL — single-attribute test push.

Pushes `engagement_tier_test` attribute to 5 representative contacts via
Track API (PUT /api/v1/customers/<email>). Use to validate end-to-end
plumbing (BQ → CIO) before building the full reverse ETL pipeline.

Usage:
    export CIO_SITE_ID="..."
    export CIO_API_KEY="..."
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python3 ingestion/scripts/customerio_push_tier_test.py [--dry-run]

Attribute pushed: engagement_tier_test (intentionally test suffix to avoid
collision with any future live engagement_tier attribute).
"""
import argparse
import os
import sys

import requests
from google.cloud import bigquery

_LOCAL_KEY = "/Users/inc42/OldMacBackup/Documents/inc42-data-warehouse/.secrets/bq-service-account.json"
_PROD_KEY = "/secrets/bq-service-account.json"
os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    _LOCAL_KEY if os.path.exists(_LOCAL_KEY) else _PROD_KEY,
)
BQ_PROJECT = "bigquery-296406"
COHORT_SIZE = 20  # contacts per tier (sort of — see cohort query)

SITE_ID = os.environ.get("CIO_SITE_ID")
API_KEY = os.environ.get("CIO_API_KEY")
if not SITE_ID or not API_KEY:
    sys.exit("ERROR: set CIO_SITE_ID and CIO_API_KEY env vars first")

TRACK_API = "https://track.customer.io/api/v1"
ATTRIBUTE_NAME = "engagement_tier_test"

# Test cohort — 5 clean, real-looking contacts per tier (20 total).
# Filters out template-placeholder emails (%spinfile%...) and ones starting
# with non-alphanumeric chars; deterministic by alphabetical email so the
# same set is picked on re-runs.
COHORT_QUERY = f"""
WITH ranked AS (
  SELECT
    LOWER(email) AS email, engagement_tier, full_name,
    ROW_NUMBER() OVER (PARTITION BY engagement_tier ORDER BY email) AS rn
  FROM `{BQ_PROJECT}.silver_gold.contact_360`
  WHERE email IS NOT NULL AND email != ''
    AND engagement_tier IN ('hot','engaged','passive','dormant')
    AND REGEXP_CONTAINS(LOWER(email), r'^[a-z0-9][a-z0-9._-]*@[a-z0-9][a-z0-9.-]*\\.[a-z]{{2,}}$')
    AND NOT REGEXP_CONTAINS(email, r'%')
    AND email NOT LIKE 'test%'
)
SELECT email, engagement_tier, full_name FROM ranked WHERE rn <= 5
ORDER BY engagement_tier, email
"""


def push(email: str, tier: str, dry_run: bool):
    url = f"{TRACK_API}/customers/{email}"
    body = {ATTRIBUTE_NAME: tier}
    if dry_run:
        print(f"  [DRY] PUT {url} body={body}")
        return True
    r = requests.put(url, json=body, auth=(SITE_ID, API_KEY), timeout=15)
    ok = 200 <= r.status_code < 300
    status = "✓" if ok else "✗"
    print(f"  {status} {r.status_code} {email:50s} {tier}")
    if not ok:
        print(f"      response: {r.text[:200]}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print what would happen, don't push")
    args = ap.parse_args()

    bq = bigquery.Client(project=BQ_PROJECT)
    rows = list(bq.query(COHORT_QUERY).result())
    print(f"\nCohort ({len(rows)} contacts):")
    for r in rows:
        print(f"  {r.engagement_tier:8s} {r.email:50s} {r.full_name or '(no name)'}")

    print(f"\nPushing `{ATTRIBUTE_NAME}` to CIO Track API"
          f"{' (DRY RUN)' if args.dry_run else ''}:")
    ok_n = sum(push(r.email, r.engagement_tier, args.dry_run) for r in rows)
    print(f"\nDone: {ok_n}/{len(rows)} succeeded")
    print(f"Verify in CIO: search any of these emails → People → Attributes → `{ATTRIBUTE_NAME}`")


if __name__ == "__main__":
    main()
