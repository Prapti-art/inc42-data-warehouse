"""
Tally.so → BigQuery Bronze Ingestion

Fetches ALL form submissions from Tally API and loads into bronze.tally_forms.

Usage:
    export TALLY_API_KEY="your_tally_api_key"
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/bq-service-account.json"
    python ingestion/scripts/tally_ingest.py

API Docs: https://developers.tally.so/api-reference/introduction
Rate Limit: 100 requests/minute
"""

import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery

# ── Config ──
TALLY_API_KEY = os.environ.get("TALLY_API_KEY")
if not TALLY_API_KEY:
    raise ValueError("Set TALLY_API_KEY environment variable. Get it from: https://tally.so/settings/api")

BQ_PROJECT = "bigquery-296406"
BQ_TABLE = f"{BQ_PROJECT}.bronze.tally_forms"
TALLY_BASE_URL = "https://api.tally.so"

HEADERS = {
    "Authorization": f"Bearer {TALLY_API_KEY}",
    "Content-Type": "application/json",
}

bq = bigquery.Client(project=BQ_PROJECT)


# ── Step 1: Get all forms ──
def get_all_forms():
    """Fetch all forms from Tally account."""
    print("📋 Fetching all forms...")
    all_forms = []
    page = 1

    while True:
        resp = requests.get(
            f"{TALLY_BASE_URL}/forms",
            headers=HEADERS,
            params={"page": page, "limit": 50}
        )
        resp.raise_for_status()
        data = resp.json()

        # Tally API returns forms in "items" key
        forms = data.get("items", data.get("forms", []))
        if isinstance(forms, list):
            all_forms.extend(forms)

        # Check pagination
        has_more = data.get("hasMore", False)
        if not has_more or not forms:
            break
        page += 1

    print(f"  ✓ Found {len(all_forms)} forms")
    return all_forms


# ── Step 2: Get submissions for a form ──
def get_form_submissions(form_id, form_name, start_date=None):
    """Fetch all submissions for a specific form, with pagination."""
    print(f"  📥 Fetching submissions for: {form_name} ({form_id})")
    all_submissions = []
    page = 1

    while True:
        params = {
            "page": page,
            "limit": 500,       # max per page
            "filter": "completed",  # only completed submissions
        }
        if start_date:
            params["startDate"] = start_date

        resp = requests.get(
            f"{TALLY_BASE_URL}/forms/{form_id}/submissions",
            headers=HEADERS,
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()

        submissions = data.get("submissions", [])
        questions = data.get("questions", [])
        all_submissions.extend(submissions)

        print(f"    Page {page}: {len(submissions)} submissions")

        has_more = data.get("hasMore", False)
        if not has_more or not submissions:
            break
        page += 1

    print(f"    ✓ Total: {len(all_submissions)} submissions")
    return all_submissions, questions


# ── Step 3: Flatten submission into a row ──
def flatten_submission(submission, form_id, form_name, questions):
    """Convert a Tally submission + its responses into a flat dict for BigQuery."""

    # Build question lookup: questionId → question label
    question_map = {}
    for q in questions:
        qid = q.get("id", "")
        label = q.get("title", q.get("label", q.get("name", qid)))
        qtype = q.get("type", "UNKNOWN")
        question_map[qid] = {"label": label, "type": qtype}

    # Extract responses into a flat dict
    answers = {}
    for resp in submission.get("responses", []):
        qid = resp.get("questionId", "")
        q_info = question_map.get(qid, {"label": qid, "type": "UNKNOWN"})
        label = q_info["label"]

        # Use formattedAnswer if available (human-readable), else raw answer
        value = resp.get("formattedAnswer") or resp.get("answer")
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        elif value is not None:
            value = str(value)

        answers[label] = value

    # Build the Bronze row
    row = {
        "response_id": submission.get("id"),
        "form_id": form_id,
        "form_name": form_name,
        "submitted_at": submission.get("submittedAt"),
        "is_completed": submission.get("isCompleted", True),

        # Try to extract common contact fields from answers
        # These field names may vary by form — we try common patterns
        "first_name": (
            answers.get("First Name") or answers.get("First name")
            or answers.get("first_name") or answers.get("Name")
        ),
        "last_name": (
            answers.get("Last Name") or answers.get("Last name")
            or answers.get("last_name")
        ),
        "email": (
            answers.get("Email") or answers.get("email")
            or answers.get("Work Email") or answers.get("Email Address")
            or answers.get("Email address")
        ),
        "work_email": (
            answers.get("Work Email") or answers.get("Work email")
            or answers.get("Official Email")
        ),
        "phone": (
            answers.get("Phone") or answers.get("Phone number")
            or answers.get("Phone Number") or answers.get("Mobile")
            or answers.get("Mobile Number") or answers.get("Contact Number")
        ),
        "whatsapp_number": (
            answers.get("WhatsApp Number") or answers.get("WhatsApp number")
            or answers.get("Whatsapp Number")
        ),
        "company_name": (
            answers.get("Company Name") or answers.get("Company name")
            or answers.get("Company's Name") or answers.get("Startup Name")
            or answers.get("Company")
        ),
        "designation": (
            answers.get("Designation") or answers.get("Current job title")
            or answers.get("Job Title") or answers.get("Role")
        ),
        "seniority": (
            answers.get("Seniority") or answers.get("Seniority level")
        ),
        "linkedin_url": (
            answers.get("LinkedIn") or answers.get("LinkedIn URL")
            or answers.get("LinkedIn profile URL") or answers.get("LinkedIn Profile URL")
        ),
        "city": (
            answers.get("City") or answers.get("City you live in")
            or answers.get("Location")
        ),
        "sector": (
            answers.get("Sector") or answers.get("Industry")
            or answers.get("Domain")
        ),

        # Startup-specific fields (Fast42, Griffin, etc.)
        "revenue_fy24": answers.get("Revenue FY24") or answers.get("Revenue (FY 2023-24)"),
        "revenue_fy25": answers.get("Revenue FY25") or answers.get("Revenue (FY 2024-25)"),
        "funding": answers.get("Funding") or answers.get("Total Funding Raised"),
        "valuation": answers.get("Valuation") or answers.get("Current Valuation"),
        "team_size": answers.get("Team Size") or answers.get("Team size") or answers.get("Number of Employees"),

        # Store ALL answers as JSON (nothing is lost)
        "all_answers_json": json.dumps(answers),

        # Audit columns
        "_ingested_at": datetime.utcnow().isoformat(),
        "_source": "tally_api",
        "_form_id": form_id,
    }

    return row


# ── Step 4: Create BigQuery table if not exists ──
def ensure_table_exists():
    """Create bronze.tally_forms if it doesn't exist."""
    schema = [
        bigquery.SchemaField("response_id", "STRING"),
        bigquery.SchemaField("form_id", "STRING"),
        bigquery.SchemaField("form_name", "STRING"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP"),
        bigquery.SchemaField("is_completed", "BOOLEAN"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("work_email", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("whatsapp_number", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("designation", "STRING"),
        bigquery.SchemaField("seniority", "STRING"),
        bigquery.SchemaField("linkedin_url", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("revenue_fy24", "STRING"),
        bigquery.SchemaField("revenue_fy25", "STRING"),
        bigquery.SchemaField("funding", "STRING"),
        bigquery.SchemaField("valuation", "STRING"),
        bigquery.SchemaField("team_size", "STRING"),
        bigquery.SchemaField("all_answers_json", "STRING"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
        bigquery.SchemaField("_form_id", "STRING"),
    ]

    table_ref = bigquery.Table(BQ_TABLE, schema=schema)
    table_ref.time_partitioning = bigquery.TimePartitioning(
        field="_ingested_at",
        type_=bigquery.TimePartitioningType.DAY,
    )

    try:
        bq.get_table(BQ_TABLE)
        print(f"  ✓ Table {BQ_TABLE} already exists")
    except Exception:
        bq.create_table(table_ref)
        print(f"  ✓ Created table {BQ_TABLE}")


# ── Step 5: Load rows into BigQuery ──
def load_to_bigquery(rows):
    """Insert rows into BigQuery using load job (handles dedup via response_id)."""
    if not rows:
        print("  ⚠️  No rows to load")
        return

    # Get existing response_ids to avoid duplicates
    try:
        existing = bq.query(f"SELECT DISTINCT response_id FROM `{BQ_TABLE}`").to_dataframe()
        existing_ids = set(existing["response_id"].tolist())
        print(f"  ℹ️  {len(existing_ids)} existing submissions in table")
    except Exception:
        existing_ids = set()

    # Filter out duplicates
    new_rows = [r for r in rows if r["response_id"] not in existing_ids]
    skipped = len(rows) - len(new_rows)

    if skipped > 0:
        print(f"  ⏭️  Skipping {skipped} already-ingested submissions")

    if not new_rows:
        print("  ✓ No new submissions to load")
        return

    # Load into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("response_id", "STRING"),
            bigquery.SchemaField("form_id", "STRING"),
            bigquery.SchemaField("form_name", "STRING"),
            bigquery.SchemaField("submitted_at", "TIMESTAMP"),
            bigquery.SchemaField("is_completed", "BOOLEAN"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("work_email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("whatsapp_number", "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("designation", "STRING"),
            bigquery.SchemaField("seniority", "STRING"),
            bigquery.SchemaField("linkedin_url", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("sector", "STRING"),
            bigquery.SchemaField("revenue_fy24", "STRING"),
            bigquery.SchemaField("revenue_fy25", "STRING"),
            bigquery.SchemaField("funding", "STRING"),
            bigquery.SchemaField("valuation", "STRING"),
            bigquery.SchemaField("team_size", "STRING"),
            bigquery.SchemaField("all_answers_json", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("_source", "STRING"),
            bigquery.SchemaField("_form_id", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )

    import pandas as pd
    df = pd.DataFrame(new_rows)
    df["submitted_at"] = pd.to_datetime(df["submitted_at"], errors="coerce")
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

    job = bq.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config)
    job.result()

    print(f"  ✓ Loaded {len(new_rows)} new submissions into {BQ_TABLE}")


# ── Main ──
def main():
    print("=" * 60)
    print("TALLY.SO → BIGQUERY BRONZE INGESTION")
    print("=" * 60)

    # Step 1: Ensure table exists
    print("\n📦 Step 1: Ensuring Bronze table exists...")
    ensure_table_exists()

    # Step 2: Get all forms
    print("\n📋 Step 2: Fetching all Tally forms...")
    forms = get_all_forms()

    if not forms:
        print("  ⚠️  No forms found. Check your API key.")
        return

    # Step 3: Get submissions for each form
    print("\n📥 Step 3: Fetching submissions per form...")
    all_rows = []

    for form in forms:
        form_id = form.get("id", "")
        form_name = form.get("name", form.get("title", "Unknown"))
        form_status = form.get("status", "UNKNOWN")

        # Skip draft forms
        if form_status == "DRAFT":
            print(f"  ⏭️  Skipping draft: {form_name}")
            continue

        submissions, questions = get_form_submissions(form_id, form_name)

        for sub in submissions:
            row = flatten_submission(sub, form_id, form_name, questions)
            all_rows.append(row)

    # Step 4: Load into BigQuery
    print(f"\n💾 Step 4: Loading {len(all_rows)} total submissions into BigQuery...")
    load_to_bigquery(all_rows)

    # Summary
    print("\n" + "=" * 60)
    print("✅ TALLY INGESTION COMPLETE")
    print("=" * 60)
    print(f"  Forms processed: {len(forms)}")
    print(f"  Total submissions: {len(all_rows)}")
    print(f"  Table: {BQ_TABLE}")


if __name__ == "__main__":
    main()
