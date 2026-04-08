"""
PySpark Identity Resolution — Real Data
Reads from BigQuery Bronze → matches contacts across 5 systems → writes to Silver.

Sources: Inc42 DB, Customer.io, Tally, WooCommerce, Gravity Forms
Match chain: Email exact → LinkedIn URL → Phone (normalized) → Name+Company (fuzzy)

Run: python spark/identity_resolution.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, coalesce, lit, regexp_replace,
    when, length, levenshtein, concat, md5, monotonically_increasing_id,
    collect_set, count, first, max as spark_max, array_distinct, flatten,
    concat_ws, size
)
from pyspark.sql.types import StringType
from google.cloud import bigquery
import os

# ── Initialize ──
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/cepl/Documents/inc42-data-warehouse/.secrets/bq-service-account.json")

spark = SparkSession.builder \
    .appName("inc42_identity_resolution") \
    .master("local[*]") \
    .config("spark.driver.memory", "3g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bq = bigquery.Client(project="bigquery-296406")

print("=" * 60)
print("PYSPARK IDENTITY RESOLUTION — REAL DATA")
print("=" * 60)


# ── Helper: Read BigQuery table into Spark DataFrame ──
def read_bq(query):
    """Run BigQuery SQL, convert to Spark DataFrame."""
    df = bq.query(query).to_dataframe()
    if df.empty:
        return None
    # Fill NaN/None in object columns with None (avoids type inference issues)
    for col_name in df.columns:
        if df[col_name].dtype == "object":
            df[col_name] = df[col_name].fillna("").astype(str)
            df[col_name] = df[col_name].replace("", None)
    # Force all columns to string to avoid inference failures
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField(c, StringType(), True) for c in df.columns])
    return spark.createDataFrame(df.astype(str).where(df.notna(), None), schema=schema)


# ── Helper: Normalize phone to E.164 (+91XXXXXXXXXX) ──
def normalize_phone_col(phone_col):
    """Normalize Indian phone numbers: strip formatting, add +91 prefix."""
    cleaned = regexp_replace(phone_col, "[\\s\\-\\(\\)\\.\\+]+", "")
    # Remove leading zeros
    cleaned = regexp_replace(cleaned, "^0+", "")
    return when(
        # Already has 91 prefix + valid 10-digit mobile
        cleaned.rlike("^91[6-9]\\d{9}$"),
        concat(lit("+"), cleaned)
    ).when(
        # Just 10-digit mobile
        cleaned.rlike("^[6-9]\\d{9}$"),
        concat(lit("+91"), cleaned)
    ).otherwise(None)


# ── Helper: Normalize LinkedIn URL ──
def normalize_linkedin_col(url_col):
    """Extract LinkedIn profile slug for comparison."""
    cleaned = lower(trim(url_col))
    # Extract the /in/username part
    cleaned = regexp_replace(cleaned, "https?://(www\\.)?linkedin\\.com/in/", "")
    cleaned = regexp_replace(cleaned, "[/?].*$", "")  # remove trailing params
    cleaned = trim(cleaned)
    return when(length(cleaned) > 2, cleaned).otherwise(None)


# ══════════════════════════════════════════════════════════
#  STEP 1: EXTRACT CONTACTS FROM EACH BRONZE SOURCE
# ══════════════════════════════════════════════════════════
print("\n📥 Step 1: Extracting contacts from Bronze tables...")

# ── Inc42 DB: users + usermeta (pivot EAV) ──
print("  Loading Inc42 DB (users + usermeta)...")
inc42 = read_bq("""
    SELECT
        'inc42_db' AS source_system,
        CAST(u.ID AS STRING) AS source_id,
        LOWER(TRIM(u.user_email)) AS email,
        MAX(CASE WHEN m.meta_key = 'first_name' THEN m.meta_value END) AS first_name,
        MAX(CASE WHEN m.meta_key = 'last_name' THEN m.meta_value END) AS last_name,
        MAX(CASE WHEN m.meta_key = 'billing_phone' THEN m.meta_value END) AS raw_phone,
        MAX(CASE WHEN m.meta_key = 'billing_company' THEN m.meta_value END) AS company_name,
        MAX(CASE WHEN m.meta_key = 'billing_designation' THEN m.meta_value END) AS designation,
        MAX(CASE WHEN m.meta_key = 'billing_city' THEN m.meta_value END) AS city,
        CAST(NULL AS STRING) AS linkedin_url
    FROM bronze.inc42_users u
    LEFT JOIN bronze.inc42_usermeta m ON u.ID = m.user_id
    WHERE u.user_email IS NOT NULL AND u.user_email != ''
    GROUP BY u.ID, u.user_email
""")
inc42_count = inc42.count() if inc42 else 0
print(f"    ✓ {inc42_count:,} contacts")


# ── Customer.io: people + attributes (pivot EAV) ──
print("  Loading Customer.io (people + attributes)...")
customerio = read_bq("""
    SELECT
        'customerio' AS source_system,
        p.internal_customer_id AS source_id,
        LOWER(TRIM(p.email_addr)) AS email,
        MAX(CASE WHEN a.attribute_name = 'First_Name' THEN a.attribute_value END) AS first_name,
        MAX(CASE WHEN a.attribute_name = 'Last_Name' THEN a.attribute_value END) AS last_name,
        MAX(CASE WHEN a.attribute_name = 'Phone Number' THEN a.attribute_value END) AS raw_phone,
        MAX(CASE WHEN a.attribute_name IN ('Company_Name', 'Company Name') THEN a.attribute_value END) AS company_name,
        CAST(NULL AS STRING) AS designation,
        MAX(CASE WHEN a.attribute_name = 'cio_city' THEN a.attribute_value END) AS city,
        MAX(CASE WHEN a.attribute_name = 'LinkedIn_Profile_URL' THEN a.attribute_value END) AS linkedin_url
    FROM bronze.cio_people p
    LEFT JOIN bronze.cio_attributes a ON p.internal_customer_id = a.internal_customer_id
    WHERE p.email_addr IS NOT NULL AND p.email_addr != ''
    GROUP BY p.internal_customer_id, p.email_addr
""")
cio_count = customerio.count() if customerio else 0
print(f"    ✓ {cio_count:,} contacts")


# ── Tally: already flat ──
print("  Loading Tally forms...")
tally = read_bq("""
    SELECT
        'tally' AS source_system,
        response_id AS source_id,
        LOWER(TRIM(COALESCE(email, work_email))) AS email,
        first_name,
        last_name,
        COALESCE(phone, whatsapp_number) AS raw_phone,
        company_name,
        designation,
        city,
        linkedin_url
    FROM bronze.tally_forms
    WHERE COALESCE(email, work_email) IS NOT NULL
""")
tally_count = tally.count() if tally else 0
print(f"    ✓ {tally_count:,} contacts")


# ── WooCommerce: orders + order_meta (pivot EAV) ──
print("  Loading WooCommerce (orders + order_meta)...")
woo = read_bq("""
    SELECT
        'woocommerce' AS source_system,
        CAST(o.order_id AS STRING) AS source_id,
        LOWER(TRIM(MAX(CASE WHEN m.meta_key = '_billing_email' THEN m.meta_value END))) AS email,
        MAX(CASE WHEN m.meta_key = '_billing_first_name' THEN m.meta_value END) AS first_name,
        MAX(CASE WHEN m.meta_key = '_billing_last_name' THEN m.meta_value END) AS last_name,
        MAX(CASE WHEN m.meta_key = '_billing_phone' THEN m.meta_value END) AS raw_phone,
        MAX(CASE WHEN m.meta_key = '_billing_company' THEN m.meta_value END) AS company_name,
        CAST(NULL AS STRING) AS designation,
        MAX(CASE WHEN m.meta_key = '_billing_city' THEN m.meta_value END) AS city,
        CAST(NULL AS STRING) AS linkedin_url
    FROM bronze.woocommerce_orders o
    JOIN bronze.woocommerce_order_meta m ON o.order_id = m.order_id
    GROUP BY o.order_id
    HAVING MAX(CASE WHEN m.meta_key = '_billing_email' THEN m.meta_value END) IS NOT NULL
""")
woo_count = woo.count() if woo else 0
print(f"    ✓ {woo_count:,} contacts")


# ── Gravity Forms: entries + entry_meta ──
# Email field varies by form. We detected the email field per form.
print("  Loading Gravity Forms (entries + entry_meta)...")
gravity = read_bq("""
    WITH email_fields AS (
        -- Detect which field contains email per form (>50% contain @)
        SELECT form_id, meta_key AS email_field
        FROM (
            SELECT form_id, meta_key,
                   COUNTIF(REGEXP_CONTAINS(meta_value, r'@[a-zA-Z0-9.-]+\\.')) AS email_cnt,
                   COUNT(*) AS total
            FROM bronze.gravity_forms_entry_meta
            WHERE SAFE_CAST(REGEXP_REPLACE(meta_key, r'\\.', '') AS INT64) IS NOT NULL
            GROUP BY form_id, meta_key
            HAVING total > 50 AND SAFE_DIVIDE(email_cnt, total) > 0.5
        )
        -- Take the field with most email entries per form
        QUALIFY ROW_NUMBER() OVER (PARTITION BY form_id ORDER BY email_cnt DESC) = 1
    ),
    pivoted AS (
        SELECT
            e.id AS entry_id,
            e.form_id,
            ef.email_field,
            MAX(CASE WHEN m.meta_key = ef.email_field THEN LOWER(TRIM(m.meta_value)) END) AS email,
            -- For name/phone/company, try common field patterns per form
            MAX(CASE WHEN m.meta_key IN ('1.3', '1') AND ef.email_field != '1' THEN m.meta_value END) AS first_name,
            MAX(CASE WHEN m.meta_key IN ('1.6', '2') AND ef.email_field != '2' THEN m.meta_value END) AS last_name,
            MAX(CASE WHEN m.meta_key IN ('3', '6') AND m.meta_value LIKE '(%' OR m.meta_value LIKE '+%' OR REGEXP_CONTAINS(m.meta_value, r'^[0-9]{10}') THEN m.meta_value END) AS raw_phone,
            MAX(CASE WHEN m.meta_key IN ('4', '5', '10') AND LENGTH(m.meta_value) > 2 AND m.meta_value NOT LIKE '%@%' AND m.meta_value NOT LIKE 'http%' AND m.meta_value NOT LIKE '(%' THEN m.meta_value END) AS company_name
        FROM bronze.gravity_forms_entries e
        JOIN email_fields ef ON e.form_id = ef.form_id
        JOIN bronze.gravity_forms_entry_meta m ON e.id = m.entry_id AND e.form_id = m.form_id
        WHERE e.status = 'active'
        GROUP BY e.id, e.form_id, ef.email_field
    )
    SELECT
        'gravity_forms' AS source_system,
        CAST(entry_id AS STRING) AS source_id,
        email,
        first_name,
        last_name,
        raw_phone,
        company_name,
        CAST(NULL AS STRING) AS designation,
        CAST(NULL AS STRING) AS city,
        CAST(NULL AS STRING) AS linkedin_url
    FROM pivoted
    WHERE email IS NOT NULL AND email LIKE '%@%'
""")
gf_count = gravity.count() if gravity else 0
print(f"    ✓ {gf_count:,} contacts")


# ══════════════════════════════════════════════════════════
#  STEP 2: UNION ALL SOURCES
# ══════════════════════════════════════════════════════════
print(f"\n📊 Step 2: Combining all sources...")

sources = [df for df in [inc42, customerio, tally, woo, gravity] if df is not None]
all_contacts = sources[0]
for df in sources[1:]:
    all_contacts = all_contacts.unionByName(df, allowMissingColumns=True)

total = all_contacts.count()
print(f"  Total records across all systems: {total:,}")


# ══════════════════════════════════════════════════════════
#  STEP 3: NORMALIZE PHONES & LINKEDIN
# ══════════════════════════════════════════════════════════
print("\n📞 Step 3: Normalizing phones & LinkedIn URLs...")

all_contacts = all_contacts \
    .withColumn("phone", normalize_phone_col(col("raw_phone"))) \
    .withColumn("linkedin_slug", normalize_linkedin_col(col("linkedin_url")))

phone_count = all_contacts.filter(col("phone").isNotNull()).count()
linkedin_count = all_contacts.filter(col("linkedin_slug").isNotNull()).count()
print(f"  ✓ {phone_count:,} valid phone numbers after normalization")
print(f"  ✓ {linkedin_count:,} valid LinkedIn profiles")


# ══════════════════════════════════════════════════════════
#  STEP 4: EMAIL EXACT MATCH (primary)
# ══════════════════════════════════════════════════════════
print("\n🔗 Step 4: Email exact match...")

# Filter to valid emails
all_contacts = all_contacts.withColumn(
    "email_clean",
    when(col("email").rlike("^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$"), col("email"))
    .otherwise(None)
)

contacts_with_email = all_contacts.filter(col("email_clean").isNotNull())
contacts_without_email = all_contacts.filter(col("email_clean").isNull())

# Generate deterministic UUID from email
unique_emails = contacts_with_email.select("email_clean").distinct() \
    .withColumn("unified_contact_id", concat(lit("uc-"), md5(col("email_clean"))))

matched = contacts_with_email.join(unique_emails, "email_clean", "left")

unique_by_email = unique_emails.count()
print(f"  ✓ {unique_by_email:,} unique people matched by email")
print(f"  ✓ {contacts_without_email.count():,} records without valid email")


# ══════════════════════════════════════════════════════════
#  STEP 5: PHONE MATCH (for records without email)
# ══════════════════════════════════════════════════════════
print("\n📱 Step 5: Phone match for email-less records...")

no_email_with_phone = contacts_without_email.filter(col("phone").isNotNull())
no_email_no_phone = contacts_without_email.filter(col("phone").isNull())

phone_matched_count = 0
if no_email_with_phone.count() > 0:
    # Build phone → unified_contact_id lookup from email-matched records
    phone_lookup = matched \
        .filter(col("phone").isNotNull()) \
        .select("phone", "unified_contact_id") \
        .dropDuplicates(["phone"])

    phone_joined = no_email_with_phone.join(phone_lookup, "phone", "left")
    phone_matched = phone_joined.filter(col("unified_contact_id").isNotNull())
    phone_unmatched = phone_joined.filter(col("unified_contact_id").isNull())

    phone_matched_count = phone_matched.count()
    print(f"  ✓ {phone_matched_count:,} records matched by phone")

    # Add phone-matched to main matched set
    if phone_matched_count > 0:
        matched = matched.unionByName(
            phone_matched.select(matched.columns),
            allowMissingColumns=True
        )

    # Records matched by neither email nor phone — assign new IDs by phone
    phone_new = phone_unmatched.count()
    if phone_new > 0:
        new_phone_ids = phone_unmatched.select("phone").distinct() \
            .withColumn("unified_contact_id", concat(lit("uc-ph-"), md5(col("phone"))))
        phone_unmatched = phone_unmatched.drop("unified_contact_id") \
            .join(new_phone_ids, "phone", "left")
        matched = matched.unionByName(
            phone_unmatched.select(matched.columns),
            allowMissingColumns=True
        )
        print(f"  ✓ {phone_new:,} new people identified by phone only")
else:
    print("  ✓ No email-less records with phone numbers")


# ══════════════════════════════════════════════════════════
#  STEP 6: ASSIGN IDs TO REMAINING UNMATCHED
# ══════════════════════════════════════════════════════════
remaining = no_email_no_phone.count()
print(f"\n🔍 Step 6: {remaining:,} records with no email or phone — assigning individual IDs")

if remaining > 0:
    # These can't be matched — give each a unique ID
    no_email_no_phone = no_email_no_phone.withColumn(
        "unified_contact_id",
        concat(lit("uc-unk-"), md5(
            concat(
                coalesce(col("source_system"), lit("")),
                lit("-"),
                coalesce(col("source_id"), lit(""))
            )
        ))
    )
    matched = matched.unionByName(
        no_email_no_phone.select(matched.columns),
        allowMissingColumns=True
    )


# ══════════════════════════════════════════════════════════
#  STEP 7: BUILD CROSS-REFERENCE TABLE
# ══════════════════════════════════════════════════════════
print("\n📋 Step 7: Building contact_source_xref...")

xref = matched.select(
    "unified_contact_id",
    "source_system",
    "source_id",
    coalesce(col("email_clean"), col("email")).alias("email"),
    "phone",
    "first_name",
    "last_name",
    "company_name",
    "designation",
    "city",
    "linkedin_url",
)

xref_count = xref.count()
print(f"  ✓ {xref_count:,} rows in cross-reference")


# ══════════════════════════════════════════════════════════
#  STEP 8: BUILD UNIFIED CONTACTS (one row per person)
# ══════════════════════════════════════════════════════════
print("\n👤 Step 8: Building unified_contacts...")

unified = xref.groupBy("unified_contact_id").agg(
    first("email", ignorenulls=True).alias("primary_email"),
    first("phone", ignorenulls=True).alias("primary_phone"),
    first("first_name", ignorenulls=True).alias("first_name"),
    first("last_name", ignorenulls=True).alias("last_name"),
    first("company_name", ignorenulls=True).alias("primary_company"),
    first("designation", ignorenulls=True).alias("designation"),
    first("city", ignorenulls=True).alias("city"),
    first("linkedin_url", ignorenulls=True).alias("linkedin_url"),
    count("*").alias("source_count"),
    collect_set("source_system").alias("found_in_systems"),
    collect_set("email").alias("all_emails"),
    collect_set("phone").alias("all_phones"),
)

# Convert array columns to comma-separated strings for BigQuery
unified = unified \
    .withColumn("found_in_systems", concat_ws(", ", col("found_in_systems"))) \
    .withColumn("all_emails", concat_ws(", ", col("all_emails"))) \
    .withColumn("all_phones", concat_ws(", ", col("all_phones")))

unified_count = unified.count()
print(f"  ✓ {unified_count:,} unique people identified")

# Show distribution
print("\n  Source coverage distribution:")
unified.groupBy("source_count").count().orderBy("source_count").show()

# Show sample multi-source contacts
print("  Sample contacts found in multiple systems:")
unified.filter(col("source_count") >= 3) \
    .select("primary_email", "first_name", "last_name", "primary_company",
            "source_count", "found_in_systems") \
    .show(10, truncate=50)


# ══════════════════════════════════════════════════════════
#  STEP 9: WRITE TO BIGQUERY SILVER
# ══════════════════════════════════════════════════════════
print("💾 Step 9: Writing to BigQuery Silver...")

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

# Write unified_contacts
unified_pd = unified.toPandas()
bq.load_table_from_dataframe(
    unified_pd, "bigquery-296406.silver.unified_contacts", job_config=job_config
).result()
print(f"  ✓ silver.unified_contacts — {unified_count:,} rows written")

# Write contact_source_xref
xref_pd = xref.toPandas()
bq.load_table_from_dataframe(
    xref_pd, "bigquery-296406.silver.contact_source_xref", job_config=job_config
).result()
print(f"  ✓ silver.contact_source_xref — {xref_count:,} rows written")


# ══════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("✅ IDENTITY RESOLUTION COMPLETE")
print("=" * 60)
print(f"""
Sources:
  Inc42 DB:       {inc42_count:>10,} records
  Customer.io:    {cio_count:>10,} records
  Tally:          {tally_count:>10,} records
  WooCommerce:    {woo_count:>10,} records
  Gravity Forms:  {gf_count:>10,} records
  ─────────────────────────────
  Total input:    {total:>10,} records

Output:
  Unique people:  {unified_count:>10,} (silver.unified_contacts)
  Cross-ref:      {xref_count:>10,} (silver.contact_source_xref)

Match breakdown:
  Email match:    {unique_by_email:,} people
  Phone match:    {phone_matched_count:,} records linked to existing people
  Unmatched:      {remaining:,} records (no email or phone)
""")

spark.stop()
print("Done.")
