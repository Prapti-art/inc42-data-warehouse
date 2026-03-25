# Inc42 Data Warehouse — Code Walkthrough

> Line-by-line explanation of every script. What each line does, why it's there, and what the output looks like.

---

## Table of Contents
1. [Bronze: 01_create_bronze_tables.py](#1-bronze-01_create_bronze_tablespy)
2. [PySpark: identity_resolution.py](#2-pyspark-identity_resolutionpy)
3. [dbt: How It Works](#3-dbt-how-it-works)
4. [dbt Silver: contacts.sql](#4-dbt-silver-contactssql)
5. [dbt Silver: companies.sql](#5-dbt-silver-companiessql)
6. [dbt Gold Dimensions: dim_contact.sql](#6-dbt-gold-dim_contactsql)
7. [dbt Gold Dimensions: dim_company.sql](#7-dbt-gold-dim_companysql)
8. [dbt Gold Dimensions: dim_date.sql](#8-dbt-gold-dim_datesql)
9. [dbt Gold Facts: fact_orders.sql](#9-dbt-gold-fact_orderssql)
10. [dbt Gold Facts: fact_event_attendance.sql](#10-dbt-gold-fact_event_attendancesql)
11. [dbt Gold Facts: fact_form_submissions.sql](#11-dbt-gold-fact_form_submissionssql)
12. [dbt Gold Facts: fact_marketing_touchpoints.sql](#12-dbt-gold-fact_marketing_touchpointssql)
13. [dbt Gold Wide: contact_360.sql](#13-dbt-gold-contact_360sql)
14. [dbt Gold Wide: company_360.sql](#14-dbt-gold-company_360sql)
15. [dbt Config Files](#15-dbt-config-files)
16. [dbt Tests: schema.yml](#16-dbt-tests-schemayml)
17. [Airflow: inc42_pipeline.py](#17-airflow-inc42_pipelinepy)
18. [Docker: Dockerfile + compose + entrypoint](#18-docker)

---

## 1. Bronze: 01_create_bronze_tables.py

**Location:** `scripts/01_create_bronze_tables.py`
**What it does:** Creates 8 tables in BigQuery `bronze` dataset and inserts 37 sample rows.
**Run:** `python scripts/01_create_bronze_tables.py`

### Code Breakdown

```python
from google.cloud import bigquery   # Google's BigQuery Python client
import json

# Connect to BigQuery using service account credentials
# GOOGLE_APPLICATION_CREDENTIALS env var tells it where the JSON key is
client = bigquery.Client(project="bigquery-296406")
```

**Why `bigquery.Client`?** This is Google's official Python library. It sends SQL queries to BigQuery's API. Your machine doesn't process data — BigQuery does.

```python
# Create a table using SQL DDL (Data Definition Language)
client.query("""
CREATE OR REPLACE TABLE bronze.gravity_forms (
    entry_id INT64,          -- Gravity's internal ID
    form_id INT64,           -- Which form (12 = AI Summit, 15 = GenAI, 18 = D2CX)
    form_name STRING,        -- Human-readable form name
    date_created TIMESTAMP,  -- When the entry was submitted
    first_name STRING,
    last_name STRING,
    email STRING,            -- Primary email field
    work_email STRING,       -- Some forms have a separate work email field
    phone_number STRING,     -- RAW phone — could be any format
    company_name STRING,     -- RAW company name — "FreshKart" or "FreshKart Pvt. Ltd."
    ...
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When WE loaded this row
    _source_file STRING      -- Which file it came from (for debugging)
)
""").result()
# .result() waits for the query to complete before moving on
```

**Why `CREATE OR REPLACE`?** So you can re-run the script safely. If the table exists, it drops and recreates it. In production, you'd use `CREATE IF NOT EXISTS` + `INSERT` instead.

**Why `_ingested_at` and `_source_file`?** These are audit columns. If data looks wrong, you can trace which file it came from and when it was loaded. Every Bronze table has these.

```python
# Insert sample data — note the intentional data quality issues
client.query("""
INSERT INTO bronze.gravity_forms VALUES
    (1001, 12, 'AI Summit 2025', '2025-11-14', 'Priya', 'Sharma',
     'priya@freshkart.in',           -- email (primary)
     NULL,                            -- no work_email in this form
     '9876543210',                    -- phone WITHOUT country code
     'FreshKart',                     -- company name version 1
     ...),
    (1003, 15, 'GenAI Summit', '2025-08-10', 'Priya', 'Sharma',
     NULL,                            -- NO primary email this time!
     'priya@freshkart.in',           -- used work_email instead
     '09876543210',                   -- phone WITH leading zero
     'FreshKart Private Limited',     -- company name version 2 (different!)
     ...)
""").result()
```

**Why are there intentional data quality issues?**
- Priya's phone appears as `9876543210` in one form and `09876543210` in another — PySpark must normalize both to `+919876543210`
- Priya's company appears as `FreshKart` and `FreshKart Private Limited` — company resolution must recognize these are the same
- Priya's email is in `email` in one form and `work_email` in another — the COALESCE in Silver handles this

### Bronze Table: customerio_identify (special — JSON traits)

```python
client.query("""
INSERT INTO bronze.customerio_identify VALUES
    ('cio_001', NULL,
     JSON '{"email":"priya@freshkart.in",
            "first_name":"Priya",
            "phone":"+919876543210",
            "daily_newsletter_status":"subscribed",
            "weekly_newsletter_status":"subscribed",
            "ai_shift_newsletter_status":"subscribed",
            "theoutline_newsletter_status":"unsubscribed",
            "plus_membership_type":"annual",
            "engagement_status":"active",
            "ltv":12999,
            "sessions":42}',
     ...)
""").result()
```

**Why JSON?** Customer.io stores ALL user attributes as a single JSON object called `traits`. Instead of 140+ columns, there's 1 JSON column. In Silver, dbt extracts each field using `JSON_VALUE(traits, '$.email')`.

---

## 2. PySpark: identity_resolution.py

**Location:** `spark/identity_resolution.py`
**What it does:** Reads 28 records from 6 Bronze tables → matches them to 5 unique people → writes to Silver.
**Run:** `python spark/identity_resolution.py`

### The Big Picture

```
Input:  28 records across 6 systems (gravity, tally, inc42, customerio, woo, hubspot)
        Priya appears 8 times with different data in each system

Process: Email match → Phone match → Fuzzy name+company match

Output: 5 unified people + cross-reference table mapping every source record to a person
```

### Code Breakdown — Section by Section

#### Section 1: Initialize Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, coalesce, lit, ...

spark = SparkSession.builder \
    .appName("inc42_identity_resolution") \  # name shown in Spark UI
    .master("local[*]") \                    # run locally using ALL CPU cores
    .getOrCreate()                           # create new session or reuse existing
```

**What is a SparkSession?** It's the entry point to all Spark functionality. Think of it as "starting the Spark engine." When you call `.master("local[*]")`, Spark uses your laptop's CPU cores to process data in parallel. On Dataproc, this would point to a cluster instead.

```python
bq = bigquery.Client(project="bigquery-296406")
```

**Why both Spark AND BigQuery client?** Spark does the compute (joins, regex, fuzzy matching). BigQuery client handles I/O (reading from and writing to BigQuery). We tried the Spark-BigQuery connector but it had version conflicts, so we use BigQuery Python client for I/O instead.

#### Section 2: Helper — Read BigQuery into Spark

```python
def read_bq(query):
    """Run BigQuery SQL, convert to Spark DataFrame."""
    rows = bq.query(query).to_dataframe()  # BigQuery → Pandas DataFrame
    return spark.createDataFrame(rows)      # Pandas → Spark DataFrame
```

**Why not just use Pandas?** For 28 rows, Pandas is fine. But at 100K+ rows with cross-joins (100K × 100K = 10 billion comparisons), Pandas crashes. Spark distributes this across CPU cores. We start with Spark now so the code scales later.

#### Section 3: Helper — Phone Normalization

```python
def normalize_phone_col(phone_col):
    """Strip formatting → add +91 if 10 digits Indian number."""
    # Step 1: Remove spaces, dashes, parentheses, dots
    cleaned = regexp_replace(phone_col, "[\\s\\-\\(\\)\\.]+", "")
    # "+91-9876543210" → "+919876543210"
    # "(098) 7654-3210" → "09876543210"

    # Step 2: Remove leading zeros
    cleaned = regexp_replace(cleaned, "^0+(\\d)", "$1")
    # "09876543210" → "9876543210"

    # Step 3: Determine which format and normalize
    return when(
        cleaned.rlike("^\\+91[6-9]\\d{9}$"), cleaned
        # Already E.164: "+919876543210" → keep as is
    ).when(
        cleaned.rlike("^91[6-9]\\d{9}$"),
        regexp_replace(cleaned, "^", "+")
        # Missing +: "919876543210" → "+919876543210"
    ).when(
        cleaned.rlike("^[6-9]\\d{9}$"),
        regexp_replace(cleaned, "^", "+91")
        # 10 digits: "9876543210" → "+919876543210"
    ).otherwise(None)
        # Invalid: "1234" → NULL
```

**Why `[6-9]\\d{9}`?** Indian mobile numbers start with 6, 7, 8, or 9. This regex validates that it's a real Indian mobile number, not a random string of digits.

**This is a PySpark function, not plain Python.** It runs inside Spark's engine on each row in parallel. For 100K rows, Spark splits this across 8 CPU cores — each core processes ~12.5K rows simultaneously.

#### Section 4: Load All Sources

```python
gravity = read_bq("""
    SELECT 'gravity' AS source_system,                    -- tag which system
           CAST(entry_id AS STRING) AS source_id,         -- original ID in that system
           LOWER(TRIM(COALESCE(email, work_email))) AS email,  -- pick best email, clean it
           phone_number AS raw_phone,                     -- raw phone (will normalize later)
           first_name, last_name, company_name
    FROM bronze.gravity_forms
""")
# Returns a Spark DataFrame with 5 rows
```

**Why `COALESCE(email, work_email)`?** Some Gravity forms have the email in the `email` field, others in `work_email`. COALESCE picks the first non-null value. This handles Priya's case where entry 1001 has `email='priya@freshkart.in'` but entry 1003 has `email=NULL, work_email='priya@freshkart.in'`.

**Why `LOWER(TRIM(...))`?** Emails might have trailing spaces or mixed case: `" Priya@FreshKart.in "` → `"priya@freshkart.in"`. This ensures exact matching works.

```python
# Customer.io is special — data is in JSON traits column
customerio = read_bq("""
    SELECT 'customerio' AS source_system,
           userId AS source_id,
           LOWER(TRIM(JSON_VALUE(traits, '$.email'))) AS email,
           JSON_VALUE(traits, '$.phone') AS raw_phone,
           JSON_VALUE(traits, '$.first_name') AS first_name,
           ...
    FROM bronze.customerio_identify
""")
```

**`JSON_VALUE(traits, '$.email')`** — BigQuery's function to extract a value from a JSON column. `$.email` means "the `email` field at the top level of the JSON object."

#### Section 5: Union All Sources

```python
all_contacts = gravity \
    .unionByName(tally) \
    .unionByName(inc42) \
    .unionByName(customerio) \
    .unionByName(woo) \
    .unionByName(hubspot)

total = all_contacts.count()  # 28 records
```

**`unionByName`** = SQL's `UNION ALL` but matches columns by name, not position. This is safer — if one source has columns in a different order, it still works.

**After this step, `all_contacts` has 28 rows** — every record from every system in one table, tagged with its `source_system`.

#### Section 6: Normalize Phones

```python
all_contacts = all_contacts.withColumn(
    "phone",
    normalize_phone_col(col("raw_phone"))
)
```

**`withColumn("phone", ...)`** adds a new column called `phone` (or replaces it if it exists). The original `raw_phone` column is preserved for debugging.

**Result:**
```
| source  | raw_phone        | phone (normalized) |
|---------|------------------|--------------------|
| gravity | 9876543210       | +919876543210      |
| gravity | +91-9555-123-456 | +919555123456      |
| inc42   | 09876543210      | +919876543210      |
| woo     | +91-9876543210   | +919876543210      |
| tally   | +919876543210    | +919876543210      |
```

All 5 different formats for Priya's phone → one canonical `+919876543210`.

#### Section 7: Email Exact Match

```python
contacts_with_email = all_contacts.filter(col("email").isNotNull())
contacts_without_email = all_contacts.filter(col("email").isNull())

# Get unique emails
unique_emails = contacts_with_email.select("email").distinct()

# Generate a deterministic ID for each unique email
unique_emails = unique_emails.withColumn(
    "unified_contact_id",
    concat(lit("uc-"), md5(col("email")))
)
```

**Why `md5(email)` for the ID?** It's deterministic — `md5("priya@freshkart.in")` always produces `"62b30341ef5fc9833aa104a7a3ef2a31"`. So if you re-run the script tomorrow, Priya gets the SAME ID. This is important for incremental processing — you don't want her ID changing every run.

```python
# Join back — every record gets its unified_contact_id
matched_by_email = contacts_with_email.join(unique_emails, "email", "left")
```

**This is the core identity resolution step.** All 8 records with `priya@freshkart.in` get the same `unified_contact_id`. All 6 records with `rahul@payease.io` get the same ID. Etc.

```
| source     | email              | unified_contact_id            |
|------------|--------------------|-------------------------------|
| gravity    | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |
| tally      | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |  ← SAME ID
| inc42      | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |  ← SAME ID
| customerio | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |  ← SAME ID
| woo        | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |  ← SAME ID
| hubspot    | priya@freshkart.in | uc-62b30341ef5fc9833aa104a... |  ← SAME ID
```

#### Section 8: Phone Match (for records without email)

```python
if no_email_count > 0:
    # Build a lookup: phone → unified_contact_id (from email-matched contacts)
    phone_lookup = matched_by_email \
        .filter(col("phone").isNotNull()) \
        .select("phone", "unified_contact_id") \
        .dropDuplicates(["phone"])

    # Match unmatched records by phone
    matched_by_phone = contacts_without_email \
        .filter(col("phone").isNotNull()) \
        .join(phone_lookup, "phone", "left")
```

**When does this matter?** If someone used a personal email in one system and a work email in another, but the same phone number in both. The phone match catches this. In our sample data, all records had emails, so this step was skipped.

#### Section 9: Fuzzy Name+Company Match

```python
if still_unmatched.count() > 0:
    fuzzy_matches = still_unmatched.alias("u").crossJoin(
        reference.alias("r")
    ).where(
        (levenshtein(
            lower(coalesce(col("u.first_name"), lit(""))),
            lower(coalesce(col("r.first_name"), lit("")))
        ) < 3)      # Names differ by < 3 characters
        &
        (levenshtein(
            lower(coalesce(col("u.company_name"), lit(""))),
            lower(coalesce(col("r.company_name"), lit("")))
        ) < 4)      # Company names differ by < 4 characters
    )
```

**`crossJoin`** = every row of table A × every row of table B. For 100K × 100K = 10 billion pairs. **This is where PySpark is essential** — SQL can't handle this at scale, but Spark distributes it across cores.

**`levenshtein`** = edit distance. How many single-character edits to transform one string into another:
- `"Priya"` → `"P. Sharma"` = distance 7 (too far, won't match)
- `"Priya"` → `"Priya"` = distance 0 (exact match)
- `"FreshKart"` → `"Freshkart"` = distance 1 (case difference after normalization = 0)

#### Section 10: Build Unified Contacts

```python
unified = xref.groupBy("unified_contact_id").agg(
    first("email").alias("primary_email"),           # pick first non-null email
    first("phone").alias("primary_phone"),           # pick first non-null phone
    first("first_name").alias("first_name"),
    first("last_name").alias("last_name"),
    first("company_name").alias("primary_company"),
    count("*").alias("source_count"),                # how many systems found this person
    collect_list("source_system").alias("found_in_systems"),  # which systems
)
```

**Result — 5 unique people:**
```
| unified_contact_id | email               | phone         | company   | source_count | found_in           |
|--------------------|---------------------|---------------|-----------|--------------|--------------------|
| uc-62b303...       | priya@freshkart.in  | +919876543210 | FreshKart | 8            | [gravity,tally,...] |
| uc-586a56...       | rahul@payease.io    | +919123456789 | PayEase   | 6            | [gravity,tally,...] |
| uc-929bec...       | neha@stylehaus.com  | +919555123456 | StyleHaus | 7            | [gravity,tally,...] |
| uc-f8990f...       | amit.patel@gmail.com| NULL          | CloudNine | 4            | [gravity,inc42,...] |
| uc-2eaaac...       | vikram@quickdeliver.in|+919888777666| QuickDeliver| 3           | [tally,inc42,...]   |
```

#### Section 11: Write to BigQuery

```python
unified_pd = unified.toPandas()     # Spark DataFrame → Pandas (for BQ client)
bq.load_table_from_dataframe(        # Pandas → BigQuery
    unified_pd,
    "bigquery-296406.silver.unified_contacts",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
)
```

**`WRITE_TRUNCATE`** = delete existing rows and replace with new data. This makes the script idempotent — running it twice produces the same result.

---

## 3. dbt: How It Works

### Core Concept

You write a SELECT statement in a `.sql` file. dbt does everything else:

```
You write:                              dbt generates:
───────────                             ───────────────
SELECT a, b, c                    →     CREATE TABLE gold.dim_contact AS (
FROM {{ ref('contacts') }}              SELECT a, b, c
                                        FROM `bigquery-296406`.`silver_silver`.`contacts`
                                        )
```

### The `{{ }}` Syntax (Jinja Templating)

dbt uses Jinja2 templating. The `{{ }}` blocks are replaced at compile time:

| Syntax | What It Does | Replaced With |
|---|---|---|
| `{{ source('bronze', 'gravity_forms') }}` | Reference a raw source table | `bigquery-296406.bronze.gravity_forms` |
| `{{ ref('contacts') }}` | Reference another dbt model | `bigquery-296406.silver_silver.contacts` |
| `{{ config(materialized='table') }}` | Set model config | (not in SQL, controls how dbt builds it) |

**Why `{{ ref() }}` instead of hardcoded table names?**
1. dbt builds a **dependency graph** from `ref()` calls
2. It knows `dim_contact` depends on `contacts`, so it runs `contacts` first
3. If you rename a table, you change it in one place — all `ref()` calls auto-update

### Execution Order

When you run `dbt run`, it:

```
1. Reads all .sql files in models/
2. Parses {{ ref() }} and {{ source() }} to build dependency graph:

   sources.yml (Bronze tables)
       ↓
   contacts.sql (Silver)  ←──── depends on Bronze + unified_contacts
   companies.sql (Silver) ←──── depends on Bronze dl_company_table
       ↓
   dim_contact.sql (Gold) ←──── depends on contacts + companies ({{ ref('contacts') }})
   dim_company.sql (Gold) ←──── depends on companies
   dim_date.sql (Gold)    ←──── no dependencies (generated data)
       ↓
   fact_orders.sql (Gold) ←──── depends on dim_contact
   fact_events.sql (Gold) ←──── depends on dim_contact
   fact_forms.sql (Gold)  ←──── depends on dim_contact
   fact_marketing.sql     ←──── depends on dim_contact
       ↓
   contact_360.sql (Gold) ←──── depends on dim_contact + ALL facts
   company_360.sql (Gold) ←──── depends on dim_company + contacts

3. Runs them in dependency order (parallelizes where possible)
4. Each model = one CREATE TABLE sent to BigQuery
```

### What `materialized='table'` Means

```yaml
# In dbt_project.yml:
models:
  inc42_warehouse:
    silver:
      +materialized: table    # ← this
```

| Materialization | What dbt Does | When to Use |
|---|---|---|
| `table` | `CREATE TABLE AS (SELECT ...)` | Most models — rebuilds fully each run |
| `view` | `CREATE VIEW AS (SELECT ...)` | Lightweight, always fresh, but slower queries |
| `incremental` | `INSERT INTO ... WHERE new_rows` | Large tables — only process new data |

We use `table` for everything now. Switch to `incremental` when data grows.

---

## 4. dbt Silver: contacts.sql

**Location:** `dbt/inc42_warehouse/models/silver/contacts.sql`
**What it does:** Creates one row per person with the best available data from all sources.

### The COALESCE Priority Pattern

```sql
-- The core pattern: pick the best value for each field
SELECT
    COALESCE(h.first_name, i.first_name, c.first_name, w.first_name) AS first_name
    --       ↑ HubSpot     ↑ Inc42      ↑ Customer.io ↑ WooCommerce
    --       (priority 1)  (priority 2) (priority 3)  (priority 4)
```

**Why this order?**
- **HubSpot first:** Sales team manually verifies contact data
- **Inc42 second:** User entered at registration (self-reported but direct)
- **Customer.io third:** Marketing system, may have enrichment
- **WooCommerce fourth:** Billing info (only people who paid)

### The CTEs (Common Table Expressions)

```sql
WITH unified AS (
    -- PySpark output: 5 rows with unified_contact_id + primary_email
    SELECT * FROM {{ source('silver', 'unified_contacts') }}
),
hubspot AS (
    -- Clean HubSpot data: lowercase email for joining
    SELECT LOWER(TRIM(email)) AS email, first_name, last_name, ...
    FROM {{ source('bronze', 'hubspot_contacts') }}
),
inc42 AS ( ... ),
customerio AS (
    -- Extract fields from JSON traits
    SELECT
        LOWER(TRIM(JSON_VALUE(traits, '$.email'))) AS email,
        JSON_VALUE(traits, '$.first_name') AS first_name,
        JSON_VALUE(traits, '$.daily_newsletter_status') AS daily_newsletter,
        CAST(JSON_VALUE(traits, '$.ltv') AS NUMERIC) AS ltv,
        ...
    FROM {{ source('bronze', 'customerio_identify') }}
),
woo AS (
    -- Deduplicate: keep only latest order per email
    SELECT email, first_name, ...
    FROM (
        SELECT ...,
            ROW_NUMBER() OVER (PARTITION BY email ORDER BY order_date DESC) AS rn
        FROM {{ source('bronze', 'woocommerce_orders') }}
    )
    WHERE rn = 1    -- ← only the most recent order per email
)
```

**Why `ROW_NUMBER() ... WHERE rn = 1` for WooCommerce?** Priya has 2 orders. Without dedup, joining contacts to WooCommerce would create 2 rows for Priya. The ROW_NUMBER picks only her latest order's billing info.

### Plus Status Logic

```sql
CASE
    WHEN c.plus_cancellation_state = 'cancelled'
         AND i.plus_expiry_date > CURRENT_DATE()
    THEN 'active_cancelling'
    -- ↑ Cancelled but still has access (expiry hasn't passed)
    -- Example: Neha cancelled in Oct, expiry is Nov 1 — she can still use Plus until Nov 1

    WHEN c.plus_cancellation_state = 'cancelled'
         AND (i.plus_expiry_date <= CURRENT_DATE() OR i.plus_expiry_date IS NULL)
    THEN 'churned'
    -- ↑ Cancelled AND expiry has passed — fully gone

    WHEN i.plus_membership_type IS NOT NULL
    THEN 'active'
    -- ↑ Has a membership type and no cancellation — active member

    ELSE NULL
    -- ↑ Not a Plus member
END AS plus_status
```

### The Final JOIN

```sql
FROM unified u                                          -- 5 rows (from PySpark)
LEFT JOIN hubspot h ON u.primary_email = h.email        -- LEFT: keep everyone, even if not in HubSpot
LEFT JOIN inc42 i ON u.primary_email = i.email
LEFT JOIN customerio c ON u.primary_email = c.email
LEFT JOIN woo w ON u.primary_email = w.email
```

**Why LEFT JOIN?** Vikram is in 3 systems (tally, inc42, customerio) but NOT in HubSpot or WooCommerce. LEFT JOIN keeps him in the result with NULL for HubSpot/WooCommerce fields. INNER JOIN would drop him.

### Output

| unified_contact_id | email | first_name | company | plus_status | daily_newsletter |
|---|---|---|---|---|---|
| uc-62b303... | priya@freshkart.in | Priya | FreshKart | active | subscribed |
| uc-586a56... | rahul@payease.io | Rahul | PayEase | NULL | subscribed |
| uc-929bec... | neha@stylehaus.com | Neha | StyleHaus | churned | subscribed |
| uc-f8990f... | amit.patel@gmail.com | Amit | CloudNine | NULL | subscribed |
| uc-2eaaac... | vikram@quickdeliver.in | Vikram | QuickDeliver | NULL | NULL |

---

## 5. dbt Silver: companies.sql

**Location:** `dbt/inc42_warehouse/models/silver/companies.sql`
**What it does:** Reads Datalabs company table, adds derived metrics.

```sql
SELECT
    dl.company_uuid AS company_id,
    dl.name AS company_name,
    ...
    -- Derived: is this company profitable?
    CASE WHEN dl.latest_pat_inr > 0 THEN TRUE ELSE FALSE END AS is_profitable,

    -- Derived: net margin percentage
    SAFE_DIVIDE(dl.latest_pat_inr, dl.latest_revenue_inr) * 100 AS net_margin_pct,
    -- SAFE_DIVIDE: returns NULL instead of error when dividing by zero
    -- FreshKart: 4200000 / 80000000 * 100 = 5.25%
    -- PayEase: -15000000 / 250000000 * 100 = -6.0% (unprofitable)

FROM {{ source('bronze', 'dl_company_table') }} dl
```

**Why `SAFE_DIVIDE`?** If a company has `latest_revenue_inr = 0`, regular division would crash. `SAFE_DIVIDE` returns NULL instead.

---

## 6. dbt Gold: dim_contact.sql

**Location:** `dbt/inc42_warehouse/models/gold/dimensions/dim_contact.sql`
**What it does:** The master contact dimension for the star schema. Company data is **DENORMALIZED INTO** the contact row.

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY c.unified_contact_id) AS contact_key,
    -- ↑ Surrogate key: integer 1, 2, 3, 4, 5
    -- Why? Integer joins are faster than UUID joins in BigQuery
    -- fact_orders.contact_key = 1 is faster than joining on a 36-char UUID

    c.unified_contact_id,   -- keep the UUID too (for reference)
    c.email,
    c.first_name,
    ...

    -- COMPANY FIELDS DENORMALIZED INTO CONTACT
    -- This is what makes it a STAR SCHEMA (not snowflake)
    co.sector AS company_sector,
    co.sub_sector AS company_sub_sector,
    co.employee_count AS company_employees,
    co.latest_revenue_inr AS company_revenue,
    co.total_funding_inr AS company_funding,
    -- ↑ Company data is COPIED into the contact row
    -- Yes, if 5 people work at FreshKart, "Consumer Brands" is stored 5 times
    -- That's intentional: queries need fewer joins → faster

FROM {{ ref('contacts') }} c
LEFT JOIN {{ ref('companies') }} co
    ON LOWER(TRIM(c.company_name)) = LOWER(TRIM(co.company_name))
```

**Star vs Snowflake in practice:**

```sql
-- STAR (our approach): 1 join to get sector
SELECT company_sector, SUM(net_revenue)
FROM fact_orders f
JOIN dim_contact c ON f.contact_key = c.contact_key
GROUP BY company_sector;
-- 1 join. Done.

-- SNOWFLAKE (if we hadn't denormalized): 2 joins
SELECT s.sector, SUM(f.net_revenue)
FROM fact_orders f
JOIN dim_contact c ON f.contact_key = c.contact_key
JOIN dim_company co ON c.company_id = co.company_id   -- extra join!
GROUP BY s.sector;
```

---

## 7. dbt Gold: dim_company.sql

Simple wrapper around Silver companies with a surrogate key:

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY company_id) AS company_key,  -- integer key
    *
FROM {{ ref('companies') }}
```

**Why a separate dim_company if company is denormalized into dim_contact?**
Because company-level fact tables (fact_company_financials, fact_funding_rounds — future) need a company dimension. Contact-level facts use dim_contact (with company inside). Company-level facts use dim_company directly.

---

## 8. dbt Gold: dim_date.sql

```sql
WITH date_spine AS (
    -- Generate every date from 2020-01-01 to 2030-12-31
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
    -- ↑ BigQuery function: creates an array of 4,018 dates
    -- UNNEST: converts array to rows (one row per date)
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
    -- ↑ 20251118 (integer) — this is what fact tables reference
    date,
    EXTRACT(YEAR FROM date) AS year,           -- 2025
    EXTRACT(QUARTER FROM date) AS quarter,     -- 4
    EXTRACT(MONTH FROM date) AS month,         -- 11
    FORMAT_DATE('%B', date) AS month_name,     -- November
    FORMAT_DATE('%A', date) AS day_name,       -- Tuesday

    -- Indian fiscal year (starts April, not January)
    CASE WHEN EXTRACT(MONTH FROM date) >= 4
        THEN EXTRACT(YEAR FROM date)       -- Apr-Dec: FY = calendar year
        ELSE EXTRACT(YEAR FROM date) - 1   -- Jan-Mar: FY = previous year
    END AS fiscal_year,
    -- March 2025 → FY 2024 (because Indian FY 2024 = Apr 2024 - Mar 2025)
    -- April 2025 → FY 2025
```

**Why a date dimension?** Instead of storing `DATE '2025-11-18'` in every fact table and computing `EXTRACT(QUARTER ...)` in every query, you store `date_key = 20251118` and JOIN to dim_date. The quarter, month name, fiscal year, weekend flag are pre-computed once.

---

## 9. dbt Gold: fact_orders.sql

```sql
WITH orders AS (
    SELECT
        o.order_id,
        o.order_total,
        o.discount_amount,
        o.tax_total,
        o.refund_amount,
        LOWER(TRIM(o.billing_email)) AS email,  -- clean for joining
        ...
    FROM {{ source('bronze', 'woocommerce_orders') }} o
)

SELECT
    o.order_id,
    dc.contact_key,    -- ← FK to dim_contact (integer, fast joins)

    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_date)) AS INT64) AS order_date_key,
    -- ↑ FK to dim_date: '2025-11-18' → 20251118

    -- MEASURES (the numbers you SUM/AVG/COUNT)
    o.order_total,
    o.order_total - COALESCE(o.discount_amount, 0) - COALESCE(o.tax_total, 0)
        AS net_revenue,
    -- ↑ ₹4999 - ₹999 (discount) - ₹763 (tax) = ₹3237 net

    COALESCE(o.refund_amount, 0) AS refund_amount,

    -- FLAGS (1 or 0 — for easy aggregation)
    CASE WHEN o.order_status = 'completed' THEN 1 ELSE 0 END AS is_completed,
    CASE WHEN o.order_status = 'refunded' THEN 1 ELSE 0 END AS is_refunded,
    CASE WHEN o.coupon_code IS NOT NULL THEN 1 ELSE 0 END AS used_coupon,

FROM orders o
LEFT JOIN {{ ref('dim_contact') }} dc ON o.email = dc.email
-- ↑ Connect order to the person via email → contact_key
```

**Why flags as 1/0?** So you can SUM them:
```sql
SELECT SUM(is_completed) AS completed_orders,
       SUM(is_refunded) AS refunded_orders,
       SUM(used_coupon) AS coupon_orders
FROM fact_orders;
```

---

## 10. dbt Gold: fact_event_attendance.sql

```sql
-- Detect cancellations by matching registrations to WooCommerce refunds
registrations AS (
    SELECT ... FROM {{ source('bronze', 'gravity_forms') }}
    WHERE event_name IS NOT NULL
),
refunds AS (
    SELECT email, refund_amount, refund_reason
    FROM {{ source('bronze', 'woocommerce_orders') }}
    WHERE refund_amount > 0    -- only refunded orders
)

SELECT
    ...
    CASE
        WHEN ref.refund_amount > 0 THEN 'cancelled'
        ELSE 'registered'
    END AS registration_status,
    -- ↑ If a matching refund exists within 60 days of registration → cancelled

FROM registrations r
LEFT JOIN refunds ref ON r.email = ref.email
    AND ABS(DATE_DIFF(DATE(r.registration_date), DATE(ref.order_date), DAY)) < 60
    -- ↑ Match refund to registration within a 60-day window
    -- Neha registered for D2CX on Oct 1, refund on Oct 5 → within window → cancelled
```

---

## 11. dbt Gold: fact_form_submissions.sql

```sql
WITH gravity_forms AS (
    SELECT
        CAST(entry_id AS STRING) AS submission_id,
        -- ↑ CAST to STRING because Tally IDs are already STRING
        -- UNION ALL requires matching types
        CAST(form_id AS STRING) AS form_id,
        -- ↑ Same: Gravity form_id is INT64, Tally form_id is STRING
        ...
        'gravity' AS source_system
    FROM {{ source('bronze', 'gravity_forms') }}
),
tally_forms AS (
    SELECT
        response_id AS submission_id,  -- already STRING
        form_id,                        -- already STRING
        ...
        'tally' AS source_system
    FROM {{ source('bronze', 'tally_forms') }}
),
all_forms AS (
    SELECT * FROM gravity_forms
    UNION ALL
    SELECT * FROM tally_forms
    -- ↑ Combine both form systems into one table
    -- UNION ALL keeps all rows (including duplicates if any)
)
```

**Why `CAST(entry_id AS STRING)`?** Gravity's `entry_id` is INT64 (e.g., `1001`). Tally's `response_id` is STRING (e.g., `"T001"`). BigQuery's `UNION ALL` requires matching types in each column position. Casting both to STRING solves this.

---

## 12. dbt Gold: fact_marketing_touchpoints.sql

```sql
WITH events AS (
    SELECT
        e.userId,
        e.event,    -- 'Email Opened', 'Email Clicked', 'Email Unsubscribed', 'Push Sent'
        JSON_VALUE(e.properties, '$.campaign_name') AS campaign_name,
        JSON_VALUE(e.properties, '$.channel') AS channel,
        -- ↑ Extract from Customer.io's JSON properties column
        e.timestamp
    FROM {{ source('bronze', 'customerio_events') }} e
),
contact_map AS (
    -- Map Customer.io userId → unified_contact_id
    SELECT source_id, unified_contact_id
    FROM {{ source('silver', 'contact_source_xref') }}
    WHERE source_system = 'customerio'
    -- ↑ The cross-reference table from PySpark tells us:
    --   cio_001 → uc-62b303... (Priya)
    --   cio_002 → uc-586a56... (Rahul)
)

SELECT
    ...
    -- Each event type becomes a 1/0 flag
    CASE WHEN ev.event = 'Email Opened' THEN 1 ELSE 0 END AS opened,
    CASE WHEN ev.event = 'Email Clicked' THEN 1 ELSE 0 END AS clicked,
    CASE WHEN ev.event = 'Email Unsubscribed' THEN 1 ELSE 0 END AS unsubscribed,

FROM events ev
LEFT JOIN contact_map cm ON ev.userId = cm.source_id
LEFT JOIN {{ ref('dim_contact') }} dc ON cm.unified_contact_id = dc.unified_contact_id
-- ↑ Two-step join: Customer.io userId → unified_contact_id → contact_key
```

**Why the two-step join?** Customer.io uses its own `userId` (e.g., `cio_001`). The fact table needs `contact_key` (e.g., `1`). The PySpark cross-reference table bridges them.

---

## 13. dbt Gold: contact_360.sql

**The most important table in the warehouse.** One row = everything about a person.

```sql
WITH contact AS (
    -- Start with dim_contact + newsletter fields from Silver contacts
    SELECT dc.*, sc.daily_newsletter, sc.weekly_newsletter, ...
    FROM {{ ref('dim_contact') }} dc
    LEFT JOIN {{ ref('contacts') }} sc ON dc.unified_contact_id = sc.unified_contact_id
),

-- Aggregate from each fact table
orders AS (
    SELECT
        contact_key,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END) AS total_revenue,
        SUM(refund_amount) AS total_refunds,
        SUM(...) - SUM(...) AS net_ltv
    FROM {{ ref('fact_orders') }}
    GROUP BY contact_key
    -- ↑ Priya has 2 orders → 1 row with total_orders=2, total_revenue=sum of both
),

events AS (
    SELECT contact_key,
        COUNT(*) AS total_events_registered,
        SUM(cancelled_flag) AS total_events_cancelled,
        ...
    FROM {{ ref('fact_event_attendance') }}
    GROUP BY contact_key
),

forms AS ( ... ),
marketing AS ( ... )

-- Final SELECT: join everything together
SELECT
    c.contact_key,
    c.email,
    c.full_name,
    c.company_name,
    c.plus_status,

    -- Orders (from orders CTE)
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.net_ltv, 0) AS net_ltv,
    -- COALESCE: if person has no orders, o.total_orders is NULL → show 0 instead

    -- Events
    COALESCE(ev.total_events_registered, 0) AS total_events_registered,

    -- Newsletter count
    (CASE WHEN c.daily_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.weekly_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.ai_shift_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + ...
    ) AS total_newsletters_subscribed,
    -- ↑ Count how many newsletters each person is subscribed to
    -- Priya: daily + weekly + ai_shift + indepth + markets = 5

    -- ENGAGEMENT SCORE (weighted composite)
    ROUND(
        COALESCE(c.sessions, 0) * 0.3           -- 42 sessions × 0.3 = 12.6
        + COALESCE(m.total_emails_opened, 0) * 2.0  -- 3 opens × 2.0 = 6.0
        + COALESCE(m.total_emails_clicked, 0) * 5.0  -- 1 click × 5.0 = 5.0
        + COALESCE(ev.total_events_registered, 0) * 10.0  -- 2 events × 10 = 20.0
        + COALESCE(f.total_form_submissions, 0) * 8.0  -- 3 forms × 8.0 = 24.0
        + COALESCE(o.total_orders, 0) * 15.0     -- 2 orders × 15.0 = 30.0
        + CASE WHEN c.plus_status = 'active' THEN 20 ELSE 0 END  -- Plus = 20
    , 1) AS engagement_score,
    -- Priya: 12.6 + 6.0 + 5.0 + 20.0 + 24.0 + 30.0 + 20.0 = 117.6

FROM contact c
LEFT JOIN orders o ON c.contact_key = o.contact_key
LEFT JOIN events ev ON c.contact_key = ev.contact_key
LEFT JOIN forms f ON c.contact_key = f.contact_key
LEFT JOIN marketing m ON c.contact_key = m.contact_key
```

**Why LEFT JOIN all fact aggregations?** Vikram has no orders. If we INNER JOIN orders, Vikram disappears. LEFT JOIN keeps everyone — people with no orders get `total_orders = 0` (via COALESCE).

---

## 14. dbt Gold: company_360.sql

```sql
-- Aggregate Inc42 contacts per company
inc42_contacts AS (
    SELECT
        company_name,
        COUNT(*) AS contacts_in_warehouse,
        SUM(CASE WHEN plus_status = 'active' THEN 1 ELSE 0 END) AS plus_members,
        SUM(ltv) AS total_ltv_from_company
    FROM {{ ref('contacts') }}
    GROUP BY company_name
)

SELECT
    co.*,   -- all Datalabs company data

    -- Inc42 engagement (how much does this company engage with Inc42?)
    COALESCE(ic.contacts_in_warehouse, 0) AS contacts_in_warehouse,
    COALESCE(ic.plus_members, 0) AS plus_members,
    COALESCE(ic.total_ltv_from_company, 0) AS total_ltv_from_company,

FROM {{ ref('dim_company') }} co
LEFT JOIN inc42_contacts ic ON LOWER(TRIM(co.company_name)) = LOWER(TRIM(ic.company_name))
```

---

## 15. dbt Config Files

### dbt_project.yml — Project Config

```yaml
name: 'inc42_warehouse'
version: '1.0.0'
profile: 'inc42_warehouse'    # ← links to profiles.yml

models:
  inc42_warehouse:
    silver:
      +schema: silver          # ← models in models/silver/ → BigQuery "silver" dataset
      +materialized: table     # ← CREATE TABLE (not VIEW)
    gold:
      dimensions:
        +schema: gold          # ← models in models/gold/dimensions/ → BigQuery "gold" dataset
        +materialized: table
      facts:
        +schema: gold
        +materialized: table
      wide:
        +schema: gold
        +materialized: table
```

**Note:** dbt appends the target schema as a prefix. So `+schema: silver` with `dataset: silver` in profiles.yml creates tables in `silver_silver` dataset. To fix this, use a custom schema macro (not done yet for simplicity).

### profiles.yml — Connection Config

```yaml
inc42_warehouse:
  target: dev                  # ← which output to use (dev vs prod)
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: bigquery-296406
      keyfile: /path/to/.secrets/bq-service-account.json
      dataset: silver          # ← default dataset for models without +schema
      location: asia-south1    # ← must match dataset location
      threads: 4               # ← run 4 models in parallel
```

### sources.yml — Source Declarations

```yaml
sources:
  - name: bronze
    database: bigquery-296406
    schema: bronze
    tables:
      - name: gravity_forms
      - name: tally_forms
      # ... all 8 Bronze tables

  - name: silver
    database: bigquery-296406
    schema: silver
    tables:
      - name: unified_contacts     # ← PySpark output
      - name: contact_source_xref  # ← PySpark output
```

**Why declare sources?** So `{{ source('bronze', 'gravity_forms') }}` works. Without this, dbt doesn't know where Bronze tables live.

---

## 16. dbt Tests: schema.yml

```yaml
models:
  - name: contacts
    columns:
      - name: unified_contact_id
        tests:
          - unique     # no duplicate IDs
          - not_null   # no NULL IDs
      - name: email
        tests:
          - unique     # no duplicate emails (one person = one email)
          - not_null   # everyone must have an email

  - name: fact_orders
    columns:
      - name: order_id
        tests:
          - not_null   # every order must have an ID
```

**What `dbt test` does with this:**

```sql
-- unique test for contacts.email:
SELECT email, COUNT(*)
FROM silver.contacts
GROUP BY email
HAVING COUNT(*) > 1;
-- 0 rows = PASS ✅ (no duplicates)
-- 1+ rows = FAIL ❌ (duplicate emails found)

-- not_null test for contacts.unified_contact_id:
SELECT COUNT(*)
FROM silver.contacts
WHERE unified_contact_id IS NULL;
-- 0 = PASS ✅
-- 1+ = FAIL ❌
```

---

## 17. Airflow: inc42_pipeline.py

**Location:** `airflow/dags/inc42_pipeline.py`

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    "owner": "data-team",
    "retries": 3,                          # try 3 times before failing
    "retry_delay": timedelta(minutes=5),   # wait 5 min between retries
    "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/secrets/bq-service-account.json",
        # ↑ Every task can access BigQuery via this env var
    },
}

with DAG(
    dag_id="inc42_data_warehouse",                  # name shown in UI
    schedule_interval="30 18 * * *",                # cron: 18:30 UTC = midnight IST
    start_date=days_ago(1),                         # start from yesterday
    catchup=False,                                  # don't backfill missed runs
    max_active_runs=1,                              # only 1 run at a time
) as dag:

    # Task 1: Load data into Bronze
    ingest_bronze = BashOperator(
        task_id="load_bronze_tables",
        bash_command="python /opt/inc42-data-warehouse/scripts/01_create_bronze_tables.py",
        # ↑ BashOperator runs a shell command
        # Same as typing this in a terminal
    )

    # Task 2: PySpark identity resolution
    spark_identity = BashOperator(
        task_id="spark_identity_resolution",
        bash_command="python /opt/inc42-data-warehouse/spark/identity_resolution.py",
    )

    # Define execution order
    ingest_bronze >> spark_identity
    # ↑ >> means "spark_identity runs AFTER ingest_bronze succeeds"
    # If ingest_bronze fails → spark_identity NEVER runs
    # If ingest_bronze fails 3 times → DAG fails → you see red in the UI
```

**The `>>` operator is Airflow's dependency syntax:**
```python
a >> b           # b runs after a
a >> [b, c]      # b and c run in parallel after a
[a, b] >> c      # c runs after BOTH a and b succeed
a >> b >> c      # sequential: a → b → c
```

---

## 18. Docker

### Dockerfile — What Each Line Does

```dockerfile
FROM python:3.11-slim
# ↑ Start with a minimal Python 3.11 image (~150MB)
# "slim" = no compilers, no man pages, just Python

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \   # Java runtime (PySpark needs it)
    curl \                       # HTTP client (for health checks)
    git \                        # Git (dbt uses it internally)
    && rm -rf /var/lib/apt/lists/*
# ↑ rm -rf cleans up apt cache to reduce image size

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
# ↑ Tell PySpark where Java is installed

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# ↑ Install all Python packages. --no-cache-dir reduces image size.
# This layer is cached — if requirements.txt doesn't change, Docker skips this step.

WORKDIR /opt/inc42-data-warehouse
# ↑ Set the working directory for all subsequent commands

COPY scripts/ scripts/
COPY spark/ spark/
COPY airflow/dags/ /opt/airflow/dags/
# ↑ Copy your code INTO the image
# NOTE: these are COPIES, not live mounts. Changes require rebuild.
# BUT docker-compose.yml overrides these with volume mounts (live sync).

EXPOSE 8080
# ↑ Document that this container listens on port 8080 (Airflow UI)

ENTRYPOINT ["/entrypoint.sh"]
# ↑ When container starts, run this script
```

### docker-compose.yml — Volume Mounts Explained

```yaml
volumes:
  # Mount secrets — NOT baked into the image (security)
  - ./.secrets:/secrets:ro
  #   ↑ local path   ↑ path inside container   ↑ read-only

  # Mount scripts — edit locally, reflected inside container INSTANTLY
  - ./scripts:/opt/inc42-data-warehouse/scripts:ro
  - ./spark:/opt/inc42-data-warehouse/spark:ro
  - ./airflow/dags:/opt/airflow/dags:ro
  # ↑ These OVERRIDE the COPY in Dockerfile
  # So you edit spark/identity_resolution.py on your laptop
  # and it's immediately available inside the container
  # No rebuild needed!

  # Named volume — persists Airflow DB across container restarts
  - airflow-db:/opt/airflow
  # ↑ Without this, stopping the container would delete:
  #   - Airflow database (all DAG run history)
  #   - Airflow admin user
  #   - You'd have to re-initialize every time
```

### entrypoint.sh — Startup Sequence

```bash
#!/bin/bash
set -e    # Exit immediately if any command fails

# First run only: initialize Airflow
if [ ! -f /opt/airflow/airflow.db ]; then
    # ↑ Check if Airflow DB exists. If not, this is the first run.

    airflow db init
    # ↑ Creates SQLite database with Airflow's schema
    # (tables for DAGs, task instances, users, connections, etc.)

    airflow users create \
        --username admin --password admin \
        --firstname Inc42 --lastname Admin \
        --role Admin --email admin@inc42.com
    # ↑ Create the admin user for the web UI
fi

airflow db migrate
# ↑ Apply any pending database migrations (for version upgrades)

airflow scheduler &
# ↑ Start the scheduler in the BACKGROUND
# The scheduler watches for:
#   - DAGs that are due to run (based on schedule_interval)
#   - Tasks that are ready to execute (dependencies met)
#   - Tasks that failed and need retrying
# The & puts it in the background so we can start the webserver too

exec airflow webserver --port 8080
# ↑ Start the webserver in the FOREGROUND
# "exec" replaces the shell process with the webserver process
# This is important for Docker — the main process must stay in foreground
# If it exits, Docker thinks the container crashed
```
