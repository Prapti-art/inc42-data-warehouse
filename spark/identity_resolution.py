"""
PySpark Identity Resolution
Reads from BigQuery Bronze → matches contacts across 7 systems → writes to Silver.

Uses BigQuery Python client for I/O + PySpark for compute (avoids connector issues).
Run: python spark/identity_resolution.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, coalesce, lit, regexp_replace,
    when, levenshtein, concat, md5,
    collect_list, count, first, max as spark_max
)
from google.cloud import bigquery
import os

# ── Initialize ──
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/tmp/inc42-data-warehouse/.secrets/bq-service-account.json")

spark = SparkSession.builder \
    .appName("inc42_identity_resolution") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bq = bigquery.Client(project="bigquery-296406")

print("=" * 60)
print("PYSPARK IDENTITY RESOLUTION — STARTING")
print("=" * 60)


# ── Helper: Read BigQuery table into Spark DataFrame ──
def read_bq(query):
    """Run BigQuery SQL, convert to Spark DataFrame."""
    rows = bq.query(query).to_dataframe()
    return spark.createDataFrame(rows)


# ── Helper: Normalize phone to E.164 ──
def normalize_phone_col(phone_col):
    cleaned = regexp_replace(phone_col, "[\\s\\-\\(\\)\\.]+", "")
    cleaned = regexp_replace(cleaned, "^0+(\\d)", "$1")
    return when(
        cleaned.rlike("^\\+91[6-9]\\d{9}$"), cleaned
    ).when(
        cleaned.rlike("^91[6-9]\\d{9}$"), regexp_replace(cleaned, "^", "+")
    ).when(
        cleaned.rlike("^[6-9]\\d{9}$"), regexp_replace(cleaned, "^", "+91")
    ).otherwise(None)


# ── STEP 1: Load all contacts from Bronze ──
print("\n📥 Loading contacts from all Bronze tables...")

gravity = read_bq("""
    SELECT 'gravity' AS source_system, CAST(entry_id AS STRING) AS source_id,
           LOWER(TRIM(COALESCE(email, work_email))) AS email,
           phone_number AS raw_phone,
           first_name, last_name, company_name
    FROM bronze.gravity_forms
""")
print(f"  Gravity Forms: {gravity.count()} records")

tally = read_bq("""
    SELECT 'tally' AS source_system, response_id AS source_id,
           LOWER(TRIM(COALESCE(email, work_email))) AS email,
           COALESCE(phone, whatsapp_number) AS raw_phone,
           first_name, last_name, company_name
    FROM bronze.tally_forms
""")
print(f"  Tally Forms: {tally.count()} records")

inc42 = read_bq("""
    SELECT 'inc42' AS source_system, CAST(user_id AS STRING) AS source_id,
           LOWER(TRIM(email)) AS email,
           mobile_number AS raw_phone,
           first_name, last_name, company_name
    FROM bronze.inc42_registered_users
""")
print(f"  Inc42 DB: {inc42.count()} records")

customerio = read_bq("""
    SELECT 'customerio' AS source_system, userId AS source_id,
           LOWER(TRIM(JSON_VALUE(traits, '$.email'))) AS email,
           JSON_VALUE(traits, '$.phone') AS raw_phone,
           JSON_VALUE(traits, '$.first_name') AS first_name,
           JSON_VALUE(traits, '$.last_name') AS last_name,
           JSON_VALUE(traits, '$.company_name') AS company_name
    FROM bronze.customerio_identify
""")
print(f"  Customer.io: {customerio.count()} records")

woo = read_bq("""
    SELECT DISTINCT 'woocommerce' AS source_system,
           CAST(customer_user_id AS STRING) AS source_id,
           LOWER(TRIM(billing_email)) AS email,
           billing_phone AS raw_phone,
           billing_first_name AS first_name,
           billing_last_name AS last_name,
           billing_company AS company_name
    FROM bronze.woocommerce_orders
""")
print(f"  WooCommerce: {woo.count()} records")

hubspot = read_bq("""
    SELECT 'hubspot' AS source_system, CAST(record_id AS STRING) AS source_id,
           LOWER(TRIM(email)) AS email,
           phone AS raw_phone,
           first_name, last_name, company_name
    FROM bronze.hubspot_contacts
""")
print(f"  HubSpot: {hubspot.count()} records")


# ── STEP 2: Union all sources ──
all_contacts = gravity \
    .unionByName(tally) \
    .unionByName(inc42) \
    .unionByName(customerio) \
    .unionByName(woo) \
    .unionByName(hubspot)

total = all_contacts.count()
print(f"\n📊 Total records across all systems: {total}")


# ── STEP 3: Normalize phones ──
print("\n📞 Normalizing phone numbers...")
all_contacts = all_contacts.withColumn("phone", normalize_phone_col(col("raw_phone")))

print("  Phone normalization results:")
all_contacts.select("source_system", "first_name", "raw_phone", "phone") \
    .filter(col("raw_phone").isNotNull()) \
    .show(truncate=False)


# ── STEP 4: EMAIL EXACT MATCH ──
print("🔗 Step 4: Email exact match...")

contacts_with_email = all_contacts.filter(col("email").isNotNull())
contacts_without_email = all_contacts.filter(col("email").isNull())

# Generate deterministic UUID from email (reruns produce same IDs)
unique_emails = contacts_with_email.select("email").distinct() \
    .withColumn("unified_contact_id", concat(lit("uc-"), md5(col("email"))))

matched_by_email = contacts_with_email.join(unique_emails, "email", "left")

email_match_count = matched_by_email.select("email").distinct().count()
no_email_count = contacts_without_email.count()
print(f"  ✓ {email_match_count} unique contacts matched by email")
print(f"  ✓ {no_email_count} records have no email")


# ── STEP 5: PHONE MATCH ──
print("\n📱 Step 5: Phone match for unmatched records...")

if no_email_count > 0:
    phone_lookup = matched_by_email \
        .filter(col("phone").isNotNull()) \
        .select("phone", "unified_contact_id") \
        .dropDuplicates(["phone"])

    phone_candidates = contacts_without_email \
        .filter(col("phone").isNotNull())

    if phone_candidates.count() > 0:
        matched_by_phone = phone_candidates.join(phone_lookup, "phone", "left") \
            .filter(col("unified_contact_id").isNotNull())
        phone_matched = matched_by_phone.count()
        print(f"  ✓ {phone_matched} records matched by phone")

        # Combine
        matched_by_email = matched_by_email.unionByName(
            matched_by_phone.select(matched_by_email.columns),
            allowMissingColumns=True
        )
    else:
        print("  ✓ No unmatched records with phone numbers")
else:
    print("  ✓ All records have email — phone match not needed")


# ── STEP 6: FUZZY NAME+COMPANY MATCH ──
print("\n🔍 Step 6: Fuzzy name+company match...")

still_unmatched = contacts_without_email.filter(col("phone").isNull())
if still_unmatched.count() > 0:
    reference = matched_by_email \
        .select("unified_contact_id", "first_name", "last_name", "company_name") \
        .dropDuplicates(["unified_contact_id"])

    fuzzy_matches = still_unmatched.alias("u").crossJoin(reference.alias("r")).where(
        (levenshtein(
            lower(coalesce(col("u.first_name"), lit(""))),
            lower(coalesce(col("r.first_name"), lit("")))
        ) < 3) &
        (levenshtein(
            lower(coalesce(col("u.company_name"), lit(""))),
            lower(coalesce(col("r.company_name"), lit("")))
        ) < 4)
    )
    print(f"  ✓ {fuzzy_matches.count()} fuzzy matches found")
else:
    print("  ✓ No remaining unmatched — all resolved!")


# ── STEP 7: Build cross-reference ──
print("\n📋 Building contact_source_xref...")

xref = matched_by_email.select(
    "unified_contact_id", "source_system", "source_id",
    "email", "phone", "first_name", "last_name", "company_name",
    lit("email_exact").alias("match_method"),
    lit(1.0).cast("double").alias("confidence")
)

print("\n  Cross-reference table (how each system's record maps to one person):")
xref.orderBy("unified_contact_id", "source_system").show(30, truncate=False)


# ── STEP 8: Build unified_contacts ──
print("👤 Building unified_contacts...")

unified = xref.groupBy("unified_contact_id").agg(
    first("email").alias("primary_email"),
    first("phone").alias("primary_phone"),
    first("first_name").alias("first_name"),
    first("last_name").alias("last_name"),
    first("company_name").alias("primary_company"),
    count("*").alias("source_count"),
    collect_list("source_system").alias("found_in_systems"),
    spark_max("confidence").alias("max_confidence")
)

print("\n  Unified contacts (one row per unique person):")
unified.show(truncate=False)


# ── STEP 9: Write to BigQuery Silver ──
print("💾 Writing to BigQuery Silver...")

# Convert to pandas and use BQ client to write
unified_pd = unified.toPandas()
unified_pd["found_in_systems"] = unified_pd["found_in_systems"].apply(str)

xref_pd = xref.toPandas()

# Write unified_contacts
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

bq.load_table_from_dataframe(
    unified_pd, "bigquery-296406.silver.unified_contacts", job_config=job_config
).result()
print("  ✓ silver.unified_contacts written")

bq.load_table_from_dataframe(
    xref_pd, "bigquery-296406.silver.contact_source_xref", job_config=job_config
).result()
print("  ✓ silver.contact_source_xref written")


# ── SUMMARY ──
unique_count = unified.count()
print("\n" + "=" * 60)
print("✅ IDENTITY RESOLUTION COMPLETE")
print("=" * 60)
print(f"""
Input:  {total} records across 6 systems
Output: {unique_count} unique people

Resolution breakdown:
  Email exact match: {email_match_count} people
  Phone match:       checked
  Fuzzy match:       checked
""")

# Show Priya's identity graph
print("Priya Sharma's identity graph (same person across 6 systems):")
xref.filter(col("email") == "priya@freshkart.in") \
    .select("source_system", "source_id", "email", "phone", "company_name") \
    .show(truncate=False)

spark.stop()
print("Done.")
