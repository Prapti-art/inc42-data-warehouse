-- Gold contact_360: ONE ROW = EVERYTHING about a person
-- This is the most important table in the warehouse

WITH contact AS (
    SELECT * FROM {{ ref('dim_contact') }}
),

orders AS (
    SELECT
        contact_key,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN is_refunded = 1 THEN net_revenue ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END)
            - SUM(CASE WHEN is_refunded = 1 THEN net_revenue ELSE 0 END) AS net_ltv,
        SUM(is_refunded) AS total_refunded_orders,
        SUM(is_cancelled) AS total_cancelled_orders,
        MAX(order_date_key) AS last_order_date_key
    FROM {{ ref('fact_orders') }}
    GROUP BY contact_key
),

events AS (
    SELECT
        contact_key,
        COUNT(*) AS total_events_registered,
        SUM(CASE WHEN registration_status = 'registered' THEN 1 ELSE 0 END) AS total_events_active,
        SUM(cancelled_flag) AS total_events_cancelled
    FROM {{ ref('fact_event_attendance') }}
    GROUP BY contact_key
),

forms AS (
    SELECT
        contact_key,
        COUNT(*) AS total_form_submissions,
        COUNT(DISTINCT form_name) AS unique_forms_submitted
    FROM {{ ref('fact_form_submissions') }}
    GROUP BY contact_key
),

marketing AS (
    SELECT
        contact_key,
        SUM(opened) AS total_emails_opened,
        SUM(clicked) AS total_emails_clicked,
        SUM(unsubscribed) AS total_unsubscribes,
        SUM(bounced) AS total_bounced,
        COUNT(*) AS total_touchpoints
    FROM {{ ref('fact_marketing_touchpoints') }}
    GROUP BY contact_key
),

-- ═══════════════════════════════════════════════
-- PROPERTY INTERACTIONS
-- ═══════════════════════════════════════════════
properties AS (
    -- Events (each event/product is a property)
    SELECT DISTINCT contact_key, event_name AS property_name, 'event' AS property_type, TRUE AS is_paid
    FROM {{ ref('fact_event_attendance') }}
    WHERE registration_status = 'registered'

    UNION ALL

    -- Forms/Programs (Fast42, Founder Survey, etc.)
    SELECT DISTINCT contact_key, form_name AS property_name, 'program' AS property_type, FALSE AS is_paid
    FROM {{ ref('fact_form_submissions') }}
),

property_agg AS (
    SELECT
        contact_key,
        COUNT(DISTINCT property_name) AS total_properties_interacted,
        STRING_AGG(DISTINCT property_name, ', ') AS properties_interacted_names,
        COUNT(DISTINCT CASE WHEN is_paid THEN property_name END) AS total_paid_properties,
        STRING_AGG(DISTINCT CASE WHEN is_paid THEN property_name END, ', ') AS paid_properties_names,
        COUNT(DISTINCT property_type) AS property_types_touched,
        STRING_AGG(DISTINCT property_type, ', ') AS property_types_names
    FROM properties
    GROUP BY contact_key
)

SELECT
    -- Identity
    c.contact_key,
    c.unified_contact_id,
    c.email,
    c.first_name,
    c.last_name,
    CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, '')) AS full_name,
    c.phone,

    -- Professional
    c.company_name,
    c.designation,
    c.seniority,
    c.linkedin_url,
    c.city,
    c.state,
    c.country,
    c.user_type,

    -- Orders
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    COALESCE(o.total_refunds, 0) AS total_refunds,
    COALESCE(o.net_ltv, 0) AS net_ltv,
    COALESCE(o.total_refunded_orders, 0) AS total_refunded_orders,
    COALESCE(o.total_cancelled_orders, 0) AS total_cancelled_orders,

    -- Events
    COALESCE(ev.total_events_registered, 0) AS total_events_registered,
    COALESCE(ev.total_events_active, 0) AS total_events_active,
    COALESCE(ev.total_events_cancelled, 0) AS total_events_cancelled,

    -- Forms
    COALESCE(f.total_form_submissions, 0) AS total_form_submissions,
    COALESCE(f.unique_forms_submitted, 0) AS unique_forms_submitted,

    -- Marketing
    COALESCE(m.total_emails_opened, 0) AS total_emails_opened,
    COALESCE(m.total_emails_clicked, 0) AS total_emails_clicked,
    COALESCE(m.total_unsubscribes, 0) AS total_unsubscribes,
    COALESCE(m.total_bounced, 0) AS total_bounced,
    COALESCE(m.total_touchpoints, 0) AS total_touchpoints,

    -- Engagement score (weighted composite)
    ROUND(
        COALESCE(m.total_emails_opened, 0) * 2.0
        + COALESCE(m.total_emails_clicked, 0) * 5.0
        + COALESCE(ev.total_events_registered, 0) * 10.0
        + COALESCE(f.total_form_submissions, 0) * 8.0
        + COALESCE(o.total_orders, 0) * 15.0
    , 1) AS engagement_score,

    -- Inc42 Property Interactions
    COALESCE(p.total_properties_interacted, 0) AS total_properties_interacted,
    p.properties_interacted_names,
    COALESCE(p.total_paid_properties, 0) AS total_paid_properties,
    p.paid_properties_names,
    COALESCE(p.property_types_touched, 0) AS property_types_touched,
    p.property_types_names,

    -- Source coverage
    c.source_count,
    c.found_in_systems,
    c.all_emails,
    c.all_phones,

    CURRENT_TIMESTAMP() AS updated_at

FROM contact c
LEFT JOIN orders o ON c.contact_key = o.contact_key
LEFT JOIN events ev ON c.contact_key = ev.contact_key
LEFT JOIN forms f ON c.contact_key = f.contact_key
LEFT JOIN marketing m ON c.contact_key = m.contact_key
LEFT JOIN property_agg p ON c.contact_key = p.contact_key
