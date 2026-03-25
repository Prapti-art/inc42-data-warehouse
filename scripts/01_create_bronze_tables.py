"""
Step 1: Create Bronze tables in BigQuery with sample data.
This simulates data from all 7 source systems.
"""

from google.cloud import bigquery
import json

client = bigquery.Client(project="bigquery-296406")

# ── BRONZE TABLE 1: Gravity Forms ──
print("Creating bronze.gravity_forms...")
client.query("""
CREATE OR REPLACE TABLE bronze.gravity_forms (
    entry_id INT64,
    form_id INT64,
    form_name STRING,
    date_created TIMESTAMP,
    first_name STRING,
    last_name STRING,
    email STRING,
    work_email STRING,
    phone_number STRING,
    company_name STRING,
    designation STRING,
    seniority STRING,
    linkedin_url STRING,
    event_name STRING,
    ticket_type STRING,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file STRING
)
""").result()

# Insert sample data
client.query("""
INSERT INTO bronze.gravity_forms (entry_id, form_id, form_name, date_created, first_name, last_name, email, work_email, phone_number, company_name, designation, seniority, linkedin_url, event_name, ticket_type, _source_file)
VALUES
    (1001, 12, 'AI Summit 2025 Registration', '2025-11-14 18:45:00', 'Priya', 'Sharma', 'priya@freshkart.in', NULL, '9876543210', 'FreshKart', 'Co-Founder & CEO', 'CXO', 'https://linkedin.com/in/priyasharma', 'AI Summit 2025', 'paid', 'gravity_export_dec2025.csv'),
    (1002, 12, 'AI Summit 2025 Registration', '2025-11-15 09:30:00', 'Rahul', 'Verma', 'rahul@payease.io', NULL, '+919123456789', 'PayEase', 'VP Engineering', 'VP', 'https://linkedin.com/in/rahulverma', 'AI Summit 2025', 'paid', 'gravity_export_dec2025.csv'),
    (1003, 15, 'GenAI Summit Registration', '2025-08-10 12:00:00', 'Priya', 'Sharma', NULL, 'priya@freshkart.in', '09876543210', 'FreshKart Private Limited', 'Co-Founder', 'CXO', NULL, 'GenAI Summit 2025', 'free', 'gravity_export_dec2025.csv'),
    (1004, 18, 'D2CX Converge Registration', '2025-10-01 14:22:00', 'Neha', 'Gupta', 'neha@stylehaus.com', NULL, '+91-9555-123-456', 'StyleHaus', 'Founder', 'CXO', 'https://linkedin.com/in/nehagupta', 'D2CX Converge 2025', 'paid', 'gravity_export_dec2025.csv'),
    (1005, 12, 'AI Summit 2025 Registration', '2025-11-16 16:00:00', 'Amit', 'Patel', 'amit.patel@gmail.com', NULL, NULL, 'CloudNine Technologies', 'CTO', 'CXO', NULL, 'AI Summit 2025', 'free', 'gravity_export_dec2025.csv')
""").result()
print("  ✓ 5 rows inserted")


# ── BRONZE TABLE 2: Tally Forms ──
print("Creating bronze.tally_forms...")
client.query("""
CREATE OR REPLACE TABLE bronze.tally_forms (
    response_id STRING,
    form_id STRING,
    form_name STRING,
    submitted_at TIMESTAMP,
    first_name STRING,
    last_name STRING,
    email STRING,
    work_email STRING,
    phone STRING,
    whatsapp_number STRING,
    company_name STRING,
    designation STRING,
    seniority STRING,
    linkedin_url STRING,
    revenue_fy24 STRING,
    revenue_fy25 STRING,
    funding STRING,
    valuation STRING,
    team_size STRING,
    city STRING,
    sector STRING,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file STRING
)
""").result()

client.query("""
INSERT INTO bronze.tally_forms VALUES
    ('T001', 'fast42_s5', 'Fast42 Season 5 Application', '2025-11-20 10:15:00', 'Priya', 'Sharma', NULL, 'priya@freshkart.in', NULL, '+919876543210', 'FreshKart Pvt. Ltd.', 'Co-Founder & CEO', 'CXO', 'https://www.linkedin.com/in/priyasharma/', '2.5 Cr', '8 Cr', '1.2 Cr (Seed)', '25 Cr', '45', 'Bengaluru', 'D2C', CURRENT_TIMESTAMP, 'tally_fast42_dec2025.csv'),
    ('T002', 'fast42_s5', 'Fast42 Season 5 Application', '2025-11-21 11:30:00', 'Rahul', 'Verma', 'rahul@payease.io', NULL, '+919123456789', NULL, 'PayEase', 'VP Engineering', 'VP', 'linkedin.com/in/rahulverma', '10 Cr', '25 Cr', '5 Cr (Series A)', '100 Cr', '120', 'Mumbai', 'Fintech', CURRENT_TIMESTAMP, 'tally_fast42_dec2025.csv'),
    ('T003', 'founder_survey', 'Founder Survey 2025', '2025-11-25 09:00:00', 'Neha', 'Gupta', 'neha@stylehaus.com', NULL, '9555123456', NULL, 'StyleHaus India', 'Founder & CEO', 'CXO', 'https://linkedin.com/in/nehagupta', '4 Cr', '12 Cr', 'Bootstrapped', 'N/A', '30', 'Delhi', 'D2C', CURRENT_TIMESTAMP, 'tally_survey_dec2025.csv'),
    ('T004', 'fast42_s5', 'Fast42 Season 5 Application', '2025-11-22 14:45:00', 'Vikram', 'Singh', 'vikram@quickdeliver.in', NULL, '+919888777666', '+919888777666', 'QuickDeliver', 'CEO', 'CXO', NULL, '50 Lakhs', '1.5 Cr', '80 Lakhs (Angel)', '8 Cr', '15', 'Jaipur', 'Logistics', CURRENT_TIMESTAMP, 'tally_fast42_dec2025.csv')
""").result()
print("  ✓ 4 rows inserted")


# ── BRONZE TABLE 3: Inc42 DB (Registered Users) ──
print("Creating bronze.inc42_registered_users...")
client.query("""
CREATE OR REPLACE TABLE bronze.inc42_registered_users (
    user_id INT64,
    email STRING,
    first_name STRING,
    last_name STRING,
    mobile_number STRING,
    company_name STRING,
    designation STRING,
    linkedin_profile_url STRING,
    city STRING,
    registered_at TIMESTAMP,
    user_type STRING,
    plus_membership_type STRING,
    plus_start_date DATE,
    plus_expiry_date DATE,
    daily_newsletter_status STRING,
    weekly_newsletter_status STRING,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file STRING
)
""").result()

client.query("""
INSERT INTO bronze.inc42_registered_users VALUES
    (5001, 'priya@freshkart.in', 'Priya', 'Sharma', '09876543210', 'FreshKart', 'Co-Founder & CEO', 'https://linkedin.com/in/priyasharma', 'Bangalore', '2024-06-12 09:30:00', 'founder', 'annual', '2025-01-15', '2026-01-15', 'subscribed', 'subscribed', CURRENT_TIMESTAMP, 'inc42_users_dec2025.csv'),
    (5002, 'rahul@payease.io', 'Rahul', 'Verma', '+919123456789', 'PayEase', 'VP Engineering', 'https://linkedin.com/in/rahulverma', 'Mumbai', '2024-08-20 14:00:00', 'professional', NULL, NULL, NULL, 'subscribed', 'unsubscribed', CURRENT_TIMESTAMP, 'inc42_users_dec2025.csv'),
    (5003, 'neha@stylehaus.com', 'Neha', 'Gupta', '09555123456', 'StyleHaus', 'Founder', NULL, 'New Delhi', '2025-03-10 11:15:00', 'founder', 'monthly', '2025-10-01', '2025-11-01', 'subscribed', 'subscribed', CURRENT_TIMESTAMP, 'inc42_users_dec2025.csv'),
    (5004, 'amit.patel@gmail.com', 'Amit', 'Patel', NULL, 'CloudNine Technologies', 'CTO', NULL, 'Hyderabad', '2025-09-01 16:30:00', 'professional', NULL, NULL, NULL, 'subscribed', NULL, CURRENT_TIMESTAMP, 'inc42_users_dec2025.csv'),
    (5005, 'vikram@quickdeliver.in', 'Vikram', 'Singh', '9888777666', 'QuickDeliver', 'CEO', NULL, 'Jaipur', '2025-07-15 10:00:00', 'founder', NULL, NULL, NULL, 'unsubscribed', NULL, CURRENT_TIMESTAMP, 'inc42_users_dec2025.csv')
""").result()
print("  ✓ 5 rows inserted")


# ── BRONZE TABLE 4: Customer.io (replaces MoEngage) ──
print("Creating bronze.customerio_identify...")
client.query("""
CREATE OR REPLACE TABLE bronze.customerio_identify (
    userId STRING,
    anonymousId STRING,
    traits JSON,
    context JSON,
    timestamp TIMESTAMP,
    receivedAt TIMESTAMP,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""").result()

client.query("""
INSERT INTO bronze.customerio_identify VALUES
    ('cio_001', NULL, JSON '{"email":"priya@freshkart.in","first_name":"Priya","last_name":"Sharma","phone":"+919876543210","company_name":"FreshKart","designation":"Co-Founder & CEO","seniority":"CXO","daily_newsletter_status":"subscribed","weekly_newsletter_status":"subscribed","ai_shift_newsletter_status":"subscribed","indepth_newsletter_status":"subscribed","theoutline_newsletter_status":"unsubscribed","markets_newsletter_status":"subscribed","plus_membership_type":"annual","plus_membership_start_date":"2025-01-15","plus_membership_expiry_date":"2026-01-15","plus_cancellation_state":null,"email_opt_in":"subscribed","whatsapp_subscription":"subscribed","engagement_status":"active","ltv":12999,"sessions":42}', JSON '{"ip":"103.21.58.1","userAgent":"Mozilla/5.0"}', '2025-12-01 22:10:00', '2025-12-01 22:10:05', CURRENT_TIMESTAMP),
    ('cio_002', NULL, JSON '{"email":"rahul@payease.io","first_name":"Rahul","last_name":"Verma","phone":"+919123456789","company_name":"PayEase","designation":"VP Engineering","daily_newsletter_status":"subscribed","weekly_newsletter_status":"unsubscribed","engagement_status":"active","ltv":0,"sessions":15}', JSON '{"ip":"49.36.12.5"}', '2025-12-01 18:30:00', '2025-12-01 18:30:02', CURRENT_TIMESTAMP),
    ('cio_003', NULL, JSON '{"email":"neha@stylehaus.com","first_name":"Neha","last_name":"Gupta","phone":"+919555123456","company_name":"StyleHaus","designation":"Founder & CEO","daily_newsletter_status":"subscribed","plus_membership_type":"monthly","plus_cancellation_state":"cancelled","engagement_status":"at_risk","ltv":4999,"sessions":8}', JSON '{"ip":"103.50.22.8"}', '2025-12-02 10:00:00', '2025-12-02 10:00:03', CURRENT_TIMESTAMP),
    ('cio_004', NULL, JSON '{"email":"amit.patel@gmail.com","first_name":"Amit","last_name":"Patel","phone":null,"company_name":"CloudNine Technologies","designation":"CTO","daily_newsletter_status":"subscribed","engagement_status":"inactive","ltv":0,"sessions":3}', JSON '{"ip":"122.15.78.9"}', '2025-11-15 09:00:00', '2025-11-15 09:00:02', CURRENT_TIMESTAMP),
    ('cio_005', 'anon_999', JSON '{"email":"vikram@quickdeliver.in","first_name":"Vikram","last_name":"Singh","phone":"+919888777666","company_name":"QuickDeliver","designation":"CEO","engagement_status":"active","ltv":0,"sessions":22}', JSON '{"ip":"59.95.100.3"}', '2025-12-01 14:00:00', '2025-12-01 14:00:01', CURRENT_TIMESTAMP)
""").result()
print("  ✓ 5 rows inserted")

# Customer.io events (track)
print("Creating bronze.customerio_events...")
client.query("""
CREATE OR REPLACE TABLE bronze.customerio_events (
    userId STRING,
    event STRING,
    properties JSON,
    context JSON,
    timestamp TIMESTAMP,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""").result()

client.query("""
INSERT INTO bronze.customerio_events VALUES
    ('cio_001', 'Email Opened', JSON '{"campaign_name":"AI Summit Reminder","subject":"AI Summit starts tomorrow!","channel":"email"}', NULL, '2025-11-19 09:15:00', CURRENT_TIMESTAMP),
    ('cio_001', 'Email Clicked', JSON '{"campaign_name":"AI Summit Reminder","link":"https://inc42.com/ai-summit","channel":"email"}', NULL, '2025-11-19 09:16:30', CURRENT_TIMESTAMP),
    ('cio_001', 'Email Opened', JSON '{"campaign_name":"Plus Renewal","subject":"Your Plus membership","channel":"email"}', NULL, '2025-12-01 08:00:00', CURRENT_TIMESTAMP),
    ('cio_002', 'Email Opened', JSON '{"campaign_name":"AI Summit Reminder","subject":"AI Summit starts tomorrow!","channel":"email"}', NULL, '2025-11-19 10:30:00', CURRENT_TIMESTAMP),
    ('cio_003', 'Email Opened', JSON '{"campaign_name":"D2CX Converge Invite","subject":"Join D2CX Converge","channel":"email"}', NULL, '2025-09-20 11:00:00', CURRENT_TIMESTAMP),
    ('cio_003', 'Email Unsubscribed', JSON '{"campaign_name":"TheOutline Newsletter","channel":"email","newsletter":"theoutline"}', NULL, '2025-10-15 14:22:00', CURRENT_TIMESTAMP),
    ('cio_001', 'Email Opened', JSON '{"campaign_name":"Fast42 Season 5","subject":"Apply to Fast42","channel":"email"}', NULL, '2025-11-18 16:00:00', CURRENT_TIMESTAMP),
    ('cio_005', 'Push Sent', JSON '{"campaign_name":"Daily Brief","channel":"web_push"}', NULL, '2025-12-01 07:00:00', CURRENT_TIMESTAMP)
""").result()
print("  ✓ 8 rows inserted")


# ── BRONZE TABLE 5: WooCommerce Orders ──
print("Creating bronze.woocommerce_orders...")
client.query("""
CREATE OR REPLACE TABLE bronze.woocommerce_orders (
    order_id INT64,
    order_date TIMESTAMP,
    order_status STRING,
    customer_user_id INT64,
    billing_email STRING,
    billing_first_name STRING,
    billing_last_name STRING,
    billing_phone STRING,
    billing_company STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    order_total NUMERIC,
    tax_total NUMERIC,
    discount_amount NUMERIC,
    coupon_code STRING,
    payment_method STRING,
    line_items_json STRING,
    refund_amount NUMERIC,
    refund_reason STRING,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file STRING
)
""").result()

client.query("""
INSERT INTO bronze.woocommerce_orders VALUES
    (88201, '2025-11-18 16:22:00', 'completed', 5001, 'priya@freshkart.in', 'Priya', 'Sharma', '+91-9876543210', 'FreshKart', 'Bengaluru', 'KA', 'IN', 4999.00, 763.41, 999.00, 'EARLY20', 'razorpay', '[{"product_id":101,"name":"AI Workshop Ticket","sku":"AIWS-2025","qty":1,"total":4999.00}]', 0, NULL, CURRENT_TIMESTAMP, 'woo_orders_dec2025.json'),
    (88202, '2025-11-18 17:10:00', 'completed', 5002, 'rahul@payease.io', 'Rahul', 'Verma', '+919123456789', 'PayEase', 'Mumbai', 'MH', 'IN', 4999.00, 763.41, 0, NULL, 'razorpay', '[{"product_id":101,"name":"AI Workshop Ticket","sku":"AIWS-2025","qty":1,"total":4999.00}]', 0, NULL, CURRENT_TIMESTAMP, 'woo_orders_dec2025.json'),
    (88203, '2025-10-05 09:30:00', 'refunded', 5003, 'neha@stylehaus.com', 'Neha', 'Gupta', '+91-9555-123-456', 'StyleHaus', 'Delhi', 'DL', 'IN', 2999.00, 457.78, 0, NULL, 'stripe', '[{"product_id":102,"name":"D2CX Converge Ticket","sku":"D2CX-2025","qty":1,"total":2999.00}]', 2999.00, 'schedule_conflict', CURRENT_TIMESTAMP, 'woo_orders_dec2025.json'),
    (88204, '2025-01-15 10:00:00', 'completed', 5001, 'priya@freshkart.in', 'Priya', 'Sharma', '+919876543210', 'FreshKart', 'Bengaluru', 'KA', 'IN', 9999.00, 1525.76, 0, NULL, 'razorpay', '[{"product_id":201,"name":"Plus Annual Membership","sku":"PLUS-ANN","qty":1,"total":9999.00}]', 0, NULL, CURRENT_TIMESTAMP, 'woo_orders_dec2025.json'),
    (88205, '2025-10-01 12:00:00', 'completed', 5003, 'neha@stylehaus.com', 'Neha', 'Gupta', '9555123456', 'StyleHaus', 'New Delhi', 'DL', 'IN', 999.00, 152.39, 0, NULL, 'razorpay', '[{"product_id":202,"name":"Plus Monthly Membership","sku":"PLUS-MON","qty":1,"total":999.00}]', 0, NULL, CURRENT_TIMESTAMP, 'woo_orders_dec2025.json')
""").result()
print("  ✓ 5 rows inserted")


# ── BRONZE TABLE 6: HubSpot Contacts ──
print("Creating bronze.hubspot_contacts...")
client.query("""
CREATE OR REPLACE TABLE bronze.hubspot_contacts (
    record_id INT64,
    email STRING,
    first_name STRING,
    last_name STRING,
    phone STRING,
    company_name STRING,
    designation STRING,
    seniority STRING,
    linkedin_url STRING,
    city STRING,
    lifecycle_stage STRING,
    lead_status STRING,
    hubspot_score INT64,
    hubspot_owner STRING,
    last_activity_date TIMESTAMP,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""").result()

client.query("""
INSERT INTO bronze.hubspot_contacts VALUES
    (50001, 'priya@freshkart.in', 'Priya', 'Sharma', '+919876543210', 'FreshKart', 'Co-Founder & CEO', 'CXO', 'https://linkedin.com/in/priyasharma', 'Bengaluru', 'opportunity', 'in_progress', 82, 'owner_jsingh', '2025-12-01 10:00:00', CURRENT_TIMESTAMP),
    (50002, 'rahul@payease.io', 'Rahul', 'Verma', '+919123456789', 'PayEase', 'VP Engineering', 'VP', 'https://linkedin.com/in/rahulverma', 'Mumbai', 'lead', 'new', 45, 'owner_jsingh', '2025-11-20 14:00:00', CURRENT_TIMESTAMP),
    (50003, 'neha@stylehaus.com', 'Neha', 'Gupta', '+919555123456', 'StyleHaus', 'Founder & CEO', 'CXO', NULL, 'Delhi', 'subscriber', NULL, 20, NULL, '2025-10-15 09:00:00', CURRENT_TIMESTAMP),
    (50004, 'amit.patel@gmail.com', 'Amit', 'Patel', NULL, 'CloudNine Technologies', 'CTO', 'CXO', NULL, 'Hyderabad', 'subscriber', NULL, 10, NULL, '2025-09-01 16:00:00', CURRENT_TIMESTAMP)
""").result()
print("  ✓ 4 rows inserted")


# ── BRONZE TABLE 7: Datalabs Company Table ──
print("Creating bronze.dl_company_table...")
client.query("""
CREATE OR REPLACE TABLE bronze.dl_company_table (
    company_uuid STRING,
    name STRING,
    legal_name STRING,
    website STRING,
    founded_year INT64,
    hq_city STRING,
    hq_state STRING,
    sector STRING,
    sub_sector STRING,
    business_model STRING,
    company_status STRING,
    employee_count INT64,
    latest_revenue_inr NUMERIC,
    latest_pat_inr NUMERIC,
    total_funding_inr NUMERIC,
    last_funding_stage STRING,
    last_funding_date DATE,
    key_investors STRING,
    monthly_web_visits INT64,
    app_rating NUMERIC,
    glassdoor_rating NUMERIC,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""").result()

client.query("""
INSERT INTO bronze.dl_company_table VALUES
    ('dl_uuid_001', 'FreshKart', 'FreshKart Private Limited', 'https://freshkart.in', 2021, 'Bengaluru', 'Karnataka', 'Consumer Brands', 'D2C', 'B2C', 'active', 45, 80000000, 4200000, 12000000, 'seed', '2023-06-15', 'Titan Capital, AngelList India, 2AM VC', 285000, 4.3, 3.8, CURRENT_TIMESTAMP),
    ('dl_uuid_002', 'PayEase', 'PayEase Technologies Private Limited', 'https://payease.io', 2019, 'Mumbai', 'Maharashtra', 'Fintech', 'Payments', 'B2B', 'active', 120, 250000000, -15000000, 500000000, 'series_a', '2024-01-20', 'Sequoia India, Y Combinator', 520000, 4.5, 4.1, CURRENT_TIMESTAMP),
    ('dl_uuid_003', 'StyleHaus', 'StyleHaus India Private Limited', 'https://stylehaus.com', 2020, 'New Delhi', 'Delhi', 'Consumer Brands', 'D2C', 'B2C', 'active', 30, 120000000, 8500000, 0, NULL, NULL, 'Bootstrapped', 180000, 4.0, 3.5, CURRENT_TIMESTAMP),
    ('dl_uuid_004', 'CloudNine Technologies', 'CloudNine Technologies Private Limited', 'https://cloudnine.tech', 2022, 'Hyderabad', 'Telangana', 'SaaS', 'DevTools', 'B2B', 'active', 18, 15000000, -5000000, 30000000, 'seed', '2024-06-01', 'Lightspeed India', 45000, NULL, NULL, CURRENT_TIMESTAMP),
    ('dl_uuid_005', 'QuickDeliver', 'QuickDeliver Logistics Private Limited', 'https://quickdeliver.in', 2023, 'Jaipur', 'Rajasthan', 'Logistics', 'Last Mile', 'B2B2C', 'active', 15, 15000000, -2000000, 8000000, 'angel', '2024-09-10', 'Mumbai Angels', 22000, 3.8, NULL, CURRENT_TIMESTAMP)
""").result()
print("  ✓ 5 rows inserted")


print("\n" + "="*60)
print("✅ ALL 7 BRONZE TABLES CREATED WITH SAMPLE DATA")
print("="*60)
print("""
Tables created:
  bronze.gravity_forms           → 5 rows (event registrations)
  bronze.tally_forms             → 4 rows (applications, surveys)
  bronze.inc42_registered_users  → 5 rows (user accounts)
  bronze.customerio_identify     → 5 rows (marketing profiles)
  bronze.customerio_events       → 8 rows (email opens, clicks, unsubs)
  bronze.woocommerce_orders      → 5 rows (orders, refunds)
  bronze.hubspot_contacts        → 4 rows (CRM leads)
  bronze.dl_company_table        → 5 rows (company intelligence)

5 sample people:
  1. Priya Sharma  — FreshKart   — appears in ALL 7 systems
  2. Rahul Verma   — PayEase     — appears in 5 systems
  3. Neha Gupta    — StyleHaus   — appears in 5 systems (Plus cancelled, event refunded)
  4. Amit Patel    — CloudNine   — appears in 4 systems (no phone!)
  5. Vikram Singh  — QuickDeliver— appears in 3 systems
""")
