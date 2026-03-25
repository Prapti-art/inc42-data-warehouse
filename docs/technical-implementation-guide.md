# Inc42 Data Warehouse — Technical Implementation Guide

> Complete step-by-step guide to replicate the entire data warehouse from scratch.
> Every command is copy-pasteable. Every tool choice is explained.

---

## 1. Project Overview

### What We Built
A data warehouse that unifies **7 source systems** into one queryable platform using the **Medallion Architecture** (Bronze → Silver → Gold) on **Google BigQuery**.

### The Problem
- Same person exists in 7 different systems with no shared key
- Same company appears as "FreshKart", "FreshKart Pvt Ltd", "FreshKart Private Limited"
- Phone numbers stored in 5+ formats: `9876543210`, `+919876543210`, `09876543210`, `+91-9876543210`
- Newsletter status duplicated 20+ times in Customer.io
- No single view of a customer or company across all touchpoints

### The Solution
```
7 Source Systems → BigQuery Bronze (raw) → PySpark (identity resolution) →
dbt Silver (cleaned) → dbt Gold (star schema + 360 views) →
Dashboards + Reverse ETL
```

### Source Systems
| Source | Data | Sync Method |
|---|---|---|
| Gravity Forms | Event registrations, applications | REST API / CSV |
| Tally | Workshops, surveys, startup applications | API / CSV |
| Inc42 DB | User accounts, newsletters, Plus memberships | DB replication |
| Customer.io | Email campaigns, engagement, push notifications | Native BigQuery connector |
| WooCommerce | Orders, payments, refunds | REST API |
| HubSpot | CRM contacts, leads, deals | CRM v3 API |
| Datalabs DB | Company intelligence (25+ tables) | DB replication |

---

## 2. Tech Stack — Why Each Tool

### BigQuery (Storage + Compute)
**What:** Google's serverless cloud data warehouse.
**Why BigQuery and not PostgreSQL?**
- Serverless — no server to manage, no indexes to tune
- Free tier: 10GB storage + 1TB queries/month (covers our needs)
- Columnar storage — queries on 1 column scan only that column
- Customer.io has a native BigQuery connector (auto-syncs every 10 min)
- dbt sends SQL to BigQuery — BigQuery does all compute, your machine does nothing

### PySpark (Heavy Compute)
**What:** Apache Spark's Python API — distributed data processing engine.
**Why PySpark and not just SQL?**
- Identity resolution requires comparing 100K × 100K contact pairs = 10 billion comparisons. BigQuery would timeout. PySpark splits this across CPU cores.
- Revenue parsing: "8 Cr" → 80000000 requires complex Python regex. SQL would need 50+ CASE WHEN statements.
- Company fuzzy matching: "FreshKart Pvt. Ltd." ≈ "FreshKart Private Limited" needs Levenshtein distance at scale.

**PySpark handles 3 jobs. dbt handles the other 30.**

### dbt Core (SQL Transformations)
**What:** You write SELECT statements. dbt turns them into CREATE TABLE, manages dependencies, runs tests, generates docs.
**Why dbt?**
- Free and open source (dbt Core, not dbt Cloud)
- Automatic dependency management: if `dim_contact` depends on `contacts`, dbt runs `contacts` first
- Built-in testing: "email must be unique", "contact_key must not be null"
- Built-in documentation with visual lineage graph
- `{{ ref('contacts') }}` — dbt resolves table names and execution order automatically

**dbt does NOT process data. It sends SQL to BigQuery. BigQuery does the work.**

### Apache Airflow (Orchestration)
**What:** Pipeline scheduler. Runs tasks in order, retries on failure, shows a visual UI.
**Why Airflow?**
- Dependency management: if ingestion fails, PySpark doesn't run
- Retries: 3 attempts with 5-minute delay before alerting
- Visual UI: see green/red for each task, click for logs
- Schedule: `30 18 * * *` = run daily at midnight IST automatically

### Docker (Packaging)
**What:** Packages Python + Java + Airflow + PySpark + dbt into one container.
**Why Docker?**
- "Works on my machine" problem eliminated
- One command to set up: `docker compose up -d`
- Same image runs on laptop, VM, or any cloud
- No manual installation of 15+ packages

### GCP VM (Cloud Hosting)
**What:** Virtual machine running Docker 24/7.
**Why a VM?**
- Airflow scheduler needs to run continuously (not request-based like Cloud Run)
- e2-medium: 2 vCPU, 4GB RAM — enough for Airflow + PySpark + dbt
- $15/month
- Close your laptop — pipeline keeps running

---

## 3. Project Structure

```
inc42-data-warehouse/
│
├── Dockerfile                          # Docker image recipe
├── docker-compose.yml                  # How to run the container
├── requirements.txt                    # Python packages
├── .gitignore                          # Excludes .secrets/, venv/, __pycache__/
│
├── .secrets/
│   └── bq-service-account.json         # GCP credentials (NEVER commit to git)
│
├── docker/
│   └── entrypoint.sh                   # Container startup: init Airflow → start scheduler → start webserver
│
├── scripts/
│   └── 01_create_bronze_tables.py      # Creates 8 Bronze tables + inserts 37 sample rows
│
├── spark/
│   └── identity_resolution.py          # PySpark: 28 records → 5 unique people
│
├── dbt/inc42_warehouse/
│   ├── dbt_project.yml                 # Project config: Silver → silver dataset, Gold → gold dataset
│   ├── profiles.yml                    # BigQuery connection (project, keyfile, location)
│   ├── models/
│   │   ├── sources.yml                 # Declares Bronze tables as dbt sources
│   │   ├── schema.yml                  # Data quality tests (unique, not_null)
│   │   ├── silver/
│   │   │   ├── contacts.sql            # One row per person, best fields from all sources
│   │   │   └── companies.sql           # Datalabs company profiles + derived metrics
│   │   └── gold/
│   │       ├── dimensions/
│   │       │   ├── dim_contact.sql     # Master contact with company DENORMALIZED in
│   │       │   ├── dim_company.sql     # Company dimension for company-level facts
│   │       │   └── dim_date.sql        # Date spine 2020-2030 with Indian fiscal year
│   │       ├── facts/
│   │       │   ├── fact_orders.sql     # One row per order
│   │       │   ├── fact_event_attendance.sql    # One row per event registration
│   │       │   ├── fact_form_submissions.sql    # One row per form submission
│   │       │   └── fact_marketing_touchpoints.sql # One row per email event
│   │       └── wide/
│   │           ├── contact_360.sql     # EVERYTHING about a person in one row
│   │           └── company_360.sql     # EVERYTHING about a company in one row
│
├── airflow/dags/
│   └── inc42_pipeline.py               # DAG: ingest → PySpark → (dbt → reverse ETL future)
│
├── docs/
│   ├── architecture.md                 # High-level architecture
│   └── architecture-detailed.md        # Detailed with Mermaid diagrams
│
└── schema/
    └── data_dictionary_final.csv       # 1000+ row field-level documentation
```

---

## 4. Step-by-Step Implementation

### 4.1 BigQuery Setup

**Prerequisites:** GCP account with a project. `gcloud` CLI installed.

```bash
# Verify you're authenticated
gcloud auth list
# Should show: * Prapti@inc42.com

# Verify project
gcloud config get-value project
# Should show: bigquery-296406

# Create the 3 medallion datasets
bq mk --location=asia-south1 --dataset bigquery-296406:bronze
bq mk --location=asia-south1 --dataset bigquery-296406:silver
bq mk --location=asia-south1 --dataset bigquery-296406:gold

# Verify
bq ls
# Should show: bronze, silver, gold (plus your existing datasets)
```

**Why `asia-south1`?** Mumbai region — closest to your users, lowest latency. All 3 datasets must be in the same region.

---

### 4.2 Service Account Setup

Python scripts need credentials to talk to BigQuery. The `gcloud` CLI uses your personal login, but scripts need a **service account**.

```bash
# Option A: Use existing service account key (what we did)
# Place the JSON key file at:
#   .secrets/bq-service-account.json

# Option B: Create a new one
# GCP Console → IAM → Service Accounts → Create
# Name: "data-warehouse-pipeline"
# Role: BigQuery Admin + Storage Admin
# Create Key → JSON → save to .secrets/

# Tell Python where the credentials are
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/.secrets/bq-service-account.json"
```

**Why a service account?** Your personal `gcloud auth` requires browser login. A service account key file can be used by scripts, Docker containers, and VMs without human interaction.

---

### 4.3 Bronze Layer — Loading Sample Data

**File:** `scripts/01_create_bronze_tables.py`

**What it does:**
1. Connects to BigQuery using the service account
2. Creates 8 tables in the `bronze` dataset (one per source system)
3. Inserts 37 sample rows across 5 fictional people

**The 5 sample people and their data quirks:**

| Person | Company | Appears In | Data Quirk |
|---|---|---|---|
| Priya Sharma | FreshKart | ALL 7 systems | Phone: 5 formats. Company: 3 spellings. Plus Annual member. |
| Rahul Verma | PayEase | 6 systems | Clean data. No data issues. |
| Neha Gupta | StyleHaus | 6 systems | Plus CANCELLED. Event REFUNDED. Company: 2 spellings. |
| Amit Patel | CloudNine | 4 systems | NO PHONE anywhere. Tests phone-less matching. |
| Vikram Singh | QuickDeliver | 3 systems | Only in Tally, Inc42, Customer.io. |

**Run it:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS=".secrets/bq-service-account.json"
python scripts/01_create_bronze_tables.py
```

**Expected output:**
```
Creating bronze.gravity_forms...
  ✓ 5 rows inserted
Creating bronze.tally_forms...
  ✓ 4 rows inserted
Creating bronze.inc42_registered_users...
  ✓ 5 rows inserted
Creating bronze.customerio_identify...
  ✓ 5 rows inserted
Creating bronze.customerio_events...
  ✓ 8 rows inserted
Creating bronze.woocommerce_orders...
  ✓ 5 rows inserted
Creating bronze.hubspot_contacts...
  ✓ 4 rows inserted
Creating bronze.dl_company_table...
  ✓ 5 rows inserted

✅ ALL 7 BRONZE TABLES CREATED WITH SAMPLE DATA
```

**Verify in BigQuery Console:**
```sql
SELECT * FROM bronze.gravity_forms LIMIT 5;
SELECT * FROM bronze.customerio_identify LIMIT 5;
```

---

### 4.4 PySpark — Identity Resolution

**File:** `spark/identity_resolution.py`

**Prerequisites:**
```bash
# Mac
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Linux / Docker
apt install openjdk-21-jre-headless
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-arm64"

# Install PySpark
pip install pyspark
```

**What the script does, step by step:**

```
Step 1: Start Spark engine locally (uses all CPU cores)
Step 2: Read 6 Bronze tables from BigQuery (via Python BigQuery client)
        → 28 total records
Step 3: Normalize ALL phone numbers to E.164 format:
        "9876543210"       → "+919876543210"
        "+91-9555-123-456" → "+919555123456"
        "09876543210"      → "+919876543210"
Step 4: Email exact match — group by email, assign unified_contact_id (md5 hash)
        → 28 records collapse to 5 unique emails
Step 5: Phone match — for records without email, match by normalized phone
        → All had emails, so this step was skipped with sample data
Step 6: Fuzzy match — crossJoin + levenshtein for remaining unmatched
        → No remaining, all resolved
Step 7: Write to BigQuery Silver:
        → silver.unified_contacts (5 rows — one per person)
        → silver.contact_source_xref (28 rows — maps every source record to a person)
```

**Run it:**
```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export GOOGLE_APPLICATION_CREDENTIALS=".secrets/bq-service-account.json"
python spark/identity_resolution.py
```

**Expected output (key parts):**
```
📊 Total records across all systems: 28

📞 Normalizing phone numbers...
+-------------+----------+----------------+-------------+
|source_system|first_name|raw_phone       |phone        |
+-------------+----------+----------------+-------------+
|gravity      |Priya     |9876543210      |+919876543210|
|gravity      |Neha      |+91-9555-123-456|+919555123456|
|inc42        |Priya     |09876543210     |+919876543210|
|woocommerce  |Priya     |+91-9876543210  |+919876543210|
+-------------+----------+----------------+-------------+

✅ IDENTITY RESOLUTION COMPLETE
Input:  28 records across 6 systems
Output: 5 unique people

Priya Sharma's identity graph (same person across 6 systems):
+-------------+---------+------------------+-------------+-------------------------+
|source_system|source_id|email             |phone        |company_name             |
+-------------+---------+------------------+-------------+-------------------------+
|gravity      |1001     |priya@freshkart.in|+919876543210|FreshKart                |
|gravity      |1003     |priya@freshkart.in|+919876543210|FreshKart Private Limited|
|tally        |T001     |priya@freshkart.in|+919876543210|FreshKart Pvt. Ltd.      |
|inc42        |5001     |priya@freshkart.in|+919876543210|FreshKart                |
|customerio   |cio_001  |priya@freshkart.in|+919876543210|FreshKart                |
|woocommerce  |5001     |priya@freshkart.in|+919876543210|FreshKart                |
|hubspot      |50001    |priya@freshkart.in|+919876543210|FreshKart                |
+-------------+---------+------------------+-------------+-------------------------+
```

**Why PySpark reads via BigQuery Python client (not Spark-BigQuery connector):**
The Spark-BigQuery connector (`spark-bigquery-with-dependencies_2.12`) had Scala version conflicts with PySpark 3.5. Using the BigQuery Python client for I/O and PySpark only for compute avoids this entirely. For production with large data, switch to the connector on Dataproc where versions are managed.

---

### 4.5 dbt — Building Silver + Gold

**Setup:**
```bash
pip install dbt-bigquery

cd dbt/inc42_warehouse

# Test BigQuery connection
dbt debug --profiles-dir .
# Should show: Connection test: OK connection ok ✅
```

**How dbt models work — the core concept:**

You write a SELECT statement. dbt handles everything else.

```sql
-- models/silver/contacts.sql
-- This is ALL you write. Just a SELECT.

SELECT
    u.unified_contact_id,
    COALESCE(h.first_name, i.first_name, c.first_name) AS first_name,
    ...
FROM {{ source('silver', 'unified_contacts') }} u
LEFT JOIN {{ source('bronze', 'hubspot_contacts') }} h ON u.primary_email = h.email
```

**What dbt does with this:**
1. Replaces `{{ source('silver', 'unified_contacts') }}` → `bigquery-296406.silver.unified_contacts`
2. Replaces `{{ source('bronze', 'hubspot_contacts') }}` → `bigquery-296406.bronze.hubspot_contacts`
3. Wraps it in: `CREATE TABLE silver.contacts AS (SELECT ...)`
4. Sends to BigQuery
5. BigQuery executes it

**The `{{ ref() }}` function — dependency management:**
```sql
-- models/gold/dimensions/dim_contact.sql
SELECT * FROM {{ ref('contacts') }}
--                  ↑ dbt knows: dim_contact depends on contacts
--                    So it runs contacts FIRST, then dim_contact
```

**Models built (11 total):**

| Layer | Model | Rows | What It Does |
|---|---|---|---|
| Silver | `contacts` | 5 | One row per person, best fields from all sources via COALESCE priority |
| Silver | `companies` | 5 | Datalabs company profiles with derived metrics (is_profitable, margins) |
| Gold | `dim_contact` | 5 | Star schema dimension — company denormalized INTO contact |
| Gold | `dim_company` | 5 | Company dimension for company-level fact tables |
| Gold | `dim_date` | 4,018 | Date spine 2020-2030, Indian fiscal year, weekend flags |
| Gold | `fact_orders` | 5 | One row per order, net_revenue computed, refund flags |
| Gold | `fact_event_attendance` | 5 | Registrations + cancellation detection via WooCommerce refunds |
| Gold | `fact_form_submissions` | 9 | All Gravity + Tally submissions unified |
| Gold | `fact_marketing_touchpoints` | 8 | Customer.io email opens, clicks, unsubs |
| Gold | `contact_360` | 5 | Everything about a person: orders, events, forms, newsletters, engagement score |
| Gold | `company_360` | 5 | Everything about a company: financials, funding, Inc42 engagement |

**Run all models:**
```bash
dbt run --profiles-dir .
```

**Expected output:**
```
Running 1 of 11: silver.contacts ........................ CREATE TABLE (5.0 rows)
Running 2 of 11: silver.companies ...................... CREATE TABLE (5.0 rows)
Running 3 of 11: gold.dim_date ......................... CREATE TABLE (4.0k rows)
Running 4 of 11: gold.dim_company ...................... CREATE TABLE (5.0 rows)
Running 5 of 11: gold.dim_contact ...................... CREATE TABLE (5.0 rows)
Running 6 of 11: gold.company_360 ...................... CREATE TABLE (5.0 rows)
Running 7 of 11: gold.fact_event_attendance ............ CREATE TABLE (5.0 rows)
Running 8 of 11: gold.fact_form_submissions ............ CREATE TABLE (9.0 rows)
Running 9 of 11: gold.fact_marketing_touchpoints ....... CREATE TABLE (8.0 rows)
Running 10 of 11: gold.fact_orders ..................... CREATE TABLE (5.0 rows)
Running 11 of 11: gold.contact_360 ..................... CREATE TABLE (5.0 rows)

Done. PASS=11 WARN=0 ERROR=0
```

**Run data quality tests:**
```bash
dbt test --profiles-dir .
```

**Expected output:**
```
1 of 15 PASS not_null_companies_company_id
2 of 15 PASS not_null_contact_360_contact_key
...
14 of 15 PASS unique_dim_company_company_key
15 of 15 PASS unique_dim_contact_contact_key

Done. PASS=15 WARN=0 ERROR=0
```

**Generate and view documentation:**
```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 9090

# Open: http://localhost:9090
# Click the green button (bottom-right) to see the lineage graph
```

**Verify in BigQuery:**
```sql
-- See Priya's complete 360 profile
SELECT full_name, company_name, plus_status, total_orders, net_ltv,
       total_events_registered, total_form_submissions,
       total_newsletters_subscribed, engagement_score
FROM gold.contact_360
ORDER BY engagement_score DESC;
```

---

### 4.6 Airflow — Orchestrating Everything

**File:** `airflow/dags/inc42_pipeline.py`

**What the DAG does:**
```
Task 1: load_bronze_tables          → Python script loads data into BigQuery Bronze
    ↓ (runs after Task 1 succeeds)
Task 2: spark_identity_resolution   → PySpark resolves identities, writes to Silver

Future tasks (commented out, ready to enable):
    ↓
Task 3: dbt_run_silver              → dbt builds Silver models
    ↓
Task 4: dbt_run_gold                → dbt builds Gold models
    ↓
Task 5: dbt_test                    → Data quality checks
    ↓
Task 6: reverse_etl                 → Push scores to Customer.io
```

**Schedule:** `30 18 * * *` = every day at 18:30 UTC = midnight IST

**Key Airflow concepts in the code:**
```python
# BashOperator — runs a shell command
ingest_bronze = BashOperator(
    task_id="load_bronze_tables",
    bash_command="python /opt/inc42-data-warehouse/scripts/01_create_bronze_tables.py",
)

# >> operator — defines execution order
ingest_bronze >> spark_identity
# Means: spark_identity runs ONLY after ingest_bronze succeeds

# Retries
default_args = {
    "retries": 3,                          # try 3 times
    "retry_delay": timedelta(minutes=5),   # wait 5 min between retries
}
```

**Airflow is NOT accessible as a standalone command.** It runs inside Docker (see next section).

---

### 4.7 Docker — Packaging Everything

**4 files make up the Docker setup:**

#### Dockerfile — The Recipe
```dockerfile
FROM python:3.11-slim                    # Start with Python 3.11

RUN apt-get install -y openjdk-21-jre-headless  # Add Java (PySpark needs it)

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt  # Install: Airflow, PySpark, dbt, BigQuery

COPY scripts/ spark/ airflow/dags/ ...    # Copy your code

ENTRYPOINT ["/entrypoint.sh"]             # Run startup script when container starts
```

#### docker-compose.yml — How to Run
```yaml
services:
  data-warehouse:
    build: .                              # Build from Dockerfile
    ports:
      - "8080:8080"                       # Airflow UI accessible at localhost:8080
    volumes:
      - ./.secrets:/secrets:ro            # Mount credentials (not baked into image)
      - ./scripts:/opt/.../scripts:ro     # Mount code (edit locally → reflects inside)
      - ./spark:/opt/.../spark:ro         # No rebuild needed for code changes
      - ./airflow/dags:/opt/airflow/dags:ro
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/secrets/bq-service-account.json
    restart: unless-stopped               # Auto-restart if container crashes
```

#### entrypoint.sh — Startup Script
```bash
# First run only: initialize Airflow database + create admin user
airflow db init
airflow users create --username admin --password admin ...

# Every run: start scheduler (background) + webserver (foreground)
airflow scheduler &
airflow webserver --port 8080
```

#### requirements.txt — Python Packages
```
google-cloud-bigquery    # BigQuery access
pyspark==3.5.3           # Spark engine
dbt-bigquery             # dbt with BigQuery adapter
apache-airflow==2.8.4    # Scheduler + web UI
requests                 # API calls
```

**Build and run locally:**
```bash
cd /Users/cepl/Documents/inc42-data-warehouse

# Build image (~3 minutes first time)
docker compose build

# Start container (runs in background)
docker compose up -d

# Access Airflow UI
open http://localhost:8080
# Username: admin | Password: admin

# View logs
docker logs -f inc42-data-warehouse

# Run PySpark manually inside container
docker exec -e JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64 \
    inc42-data-warehouse \
    python /opt/inc42-data-warehouse/spark/identity_resolution.py

# Shell into container
docker exec -it inc42-data-warehouse bash

# Stop
docker compose down
```

**When to rebuild vs restart:**

| Changed | Action |
|---|---|
| `scripts/*.py`, `spark/*.py`, `airflow/dags/*.py` | Nothing — volumes auto-sync |
| `requirements.txt` | `docker compose up -d --build` |
| `Dockerfile` | `docker compose up -d --build` |
| `.secrets/` | `docker compose restart` |

---

### 4.8 GCP VM — Cloud Deployment

**Why:** Docker on your laptop stops when you close the lid. A VM runs 24/7.

#### Create the VM
```bash
gcloud compute instances create inc42-data-warehouse \
    --project=bigquery-296406 \
    --zone=asia-south2-a \
    --machine-type=e2-medium \
    --boot-disk-size=30GB \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --tags=http-server \
    --metadata=startup-script='#!/bin/bash
apt-get update
apt-get install -y docker.io docker-compose-v2
systemctl enable docker
systemctl start docker' \
    --scopes=cloud-platform
```

**What this creates:**
- VM name: `inc42-data-warehouse`
- Region: `asia-south2-a` (Delhi)
- Size: e2-medium (2 vCPU, 4GB RAM) — ~$15/month
- OS: Ubuntu 22.04
- Auto-installs Docker on first boot

#### Open Firewall for Airflow UI
```bash
gcloud compute firewall-rules create allow-airflow-8080 \
    --project=bigquery-296406 \
    --direction=INGRESS \
    --action=ALLOW \
    --rules=tcp:8080 \
    --target-tags=http-server \
    --source-ranges=0.0.0.0/0
```

#### Push Project to VM
```bash
cd /Users/cepl/Documents/inc42-data-warehouse

gcloud compute scp --recurse \
    Dockerfile docker-compose.yml requirements.txt \
    docker/ scripts/ spark/ airflow/ ingestion/ .secrets/ \
    inc42-data-warehouse:~/inc42-data-warehouse/ \
    --zone=asia-south2-a --project=bigquery-296406
```

#### Build and Start on VM
```bash
# Build Docker image on the VM
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a \
    --command="cd ~/inc42-data-warehouse && sudo docker compose build"

# Start the container
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a \
    --command="cd ~/inc42-data-warehouse && sudo docker compose up -d"

# Verify it's running
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a \
    --command="sudo docker ps"
```

#### Access From Anywhere
```
http://34.126.220.115:8080
Username: admin
Password: admin
```
(Replace IP with your VM's external IP — find via `gcloud compute instances list`)

#### VM Management Commands
```bash
# SSH into VM
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a

# View container logs
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a \
    --command="sudo docker logs -f inc42-data-warehouse"

# Update code (after editing locally)
gcloud compute scp --recurse scripts/ spark/ airflow/ \
    inc42-data-warehouse:~/inc42-data-warehouse/ \
    --zone=asia-south2-a

# Restart container (after code update)
gcloud compute ssh inc42-data-warehouse --zone=asia-south2-a \
    --command="cd ~/inc42-data-warehouse && sudo docker compose restart"

# Stop VM (save money when not needed)
gcloud compute instances stop inc42-data-warehouse --zone=asia-south2-a

# Start VM
gcloud compute instances start inc42-data-warehouse --zone=asia-south2-a
```

---

## 5. Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: INGEST                                                      │
│ Python scripts call APIs / read databases                           │
│ Raw data → BigQuery Bronze (8 tables, 37 rows)                      │
│ Tool: Python + google-cloud-bigquery                                │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: IDENTITY RESOLUTION                                         │
│ PySpark reads Bronze → normalizes phones → matches by email →       │
│ fuzzy matches remaining → writes unified IDs to Silver              │
│ 28 records → 5 unique people                                        │
│ Tool: PySpark                                                       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: SILVER TRANSFORMS                                           │
│ dbt reads Bronze + PySpark output → COALESCE best fields →          │
│ builds clean contacts + companies tables                            │
│ Tool: dbt (sends SQL to BigQuery)                                   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: GOLD STAR SCHEMA                                            │
│ dbt reads Silver → builds dimensions + facts + 360 views            │
│ 3 dimensions + 4 facts + 2 wide tables = 11 models total            │
│ Tool: dbt (sends SQL to BigQuery)                                   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 5: DATA QUALITY                                                │
│ dbt test: 15 checks (unique, not_null) — all must pass              │
│ If any fail → pipeline stops                                        │
│ Tool: dbt test                                                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ ORCHESTRATION: Airflow schedules all steps daily at midnight IST    │
│ PACKAGING: Docker puts everything in one container                  │
│ HOSTING: GCP VM runs Docker 24/7 — no laptop dependency             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Cost Summary

| Service | Monthly Cost | Notes |
|---|---|---|
| BigQuery storage | $0 | Free tier: 10GB (we use ~3-5GB) |
| BigQuery queries | $0 | Free tier: 1TB/month (we use ~100-200GB) |
| GCS | $0 | Free tier: 5GB |
| GCP VM (e2-medium) | ~$15 | 2 vCPU, 4GB RAM, 30GB disk |
| Docker | $0 | Open source |
| dbt Core | $0 | Open source |
| Airflow | $0 | Open source |
| PySpark | $0 | Open source |
| Looker Studio | $0 | Free (unlimited) |
| GitHub | $0 | Free private repos |
| **Total** | **~$15/month** | |

**Equivalent SaaS stack:** Snowflake + Fivetran + dbt Cloud + Astronomer + Segment + Tableau = **~$1,800+/month**

---

## 7. What's Next

### Phase 2: Connect Real Sources
- [ ] WooCommerce REST API (generate `ck_`/`cs_` keys at WooCommerce → Settings → Advanced → REST API)
- [ ] HubSpot CRM v3 API (create private app for auth token)
- [ ] Gravity Forms REST API (enable at Forms → Settings → REST API)
- [ ] Customer.io native BigQuery connector (Data & Integrations → Google BigQuery Advanced)
- [ ] Inc42 DB direct replication
- [ ] Datalabs DB direct replication

### Phase 3: Complete Silver + Gold
- [ ] `silver.event_registrations` (with cancel/refund lifecycle)
- [ ] `silver.newsletter_subscriptions` (9 newsletters per contact)
- [ ] `silver.plus_memberships` (active/active_cancelling/churned)
- [ ] `gold.fact_subscription_lifecycle`
- [ ] `gold.fact_newsletter_engagement`
- [ ] `gold.fact_lead_stage_changes`
- [ ] `gold.fact_property_interactions`

### Phase 4: Reverse ETL + Dashboards
- [ ] Push `engagement_score` to Customer.io
- [ ] Build segments: churn risk, upsell, event promo
- [ ] Slack alerts: hot leads, churn risk, bounce rate
- [ ] Looker Studio dashboards connected to Gold tables

### Phase 5: Scale
- [ ] Move PySpark to Dataproc Serverless (when data > 500K contacts)
- [ ] Add dbt incremental models (process only new/changed rows)
- [ ] Add dbt snapshots (SCD Type 2 for contact history)
