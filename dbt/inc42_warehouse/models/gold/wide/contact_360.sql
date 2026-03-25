-- Gold contact_360: ONE ROW = EVERYTHING about a person
-- This is the most important table in the warehouse

WITH contact AS (
    SELECT dc.*, sc.daily_newsletter, sc.weekly_newsletter,
           sc.ai_shift_newsletter, sc.indepth_newsletter,
           sc.theoutline_newsletter, sc.markets_newsletter,
           sc.email_opt_in, sc.whatsapp_opt_in
    FROM {{ ref('dim_contact') }} dc
    LEFT JOIN {{ ref('contacts') }} sc ON dc.unified_contact_id = sc.unified_contact_id
),

orders AS (
    SELECT
        contact_key,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END) AS total_revenue,
        SUM(refund_amount) AS total_refunds,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END) - SUM(refund_amount) AS net_ltv,
        SUM(is_refunded) AS total_refunded_orders,
        MAX(order_date_key) AS last_order_date_key
    FROM {{ ref('fact_orders') }}
    GROUP BY contact_key
),

events AS (
    SELECT
        contact_key,
        COUNT(*) AS total_events_registered,
        SUM(CASE WHEN registration_status != 'cancelled' THEN 1 ELSE 0 END) AS total_events_active,
        SUM(cancelled_flag) AS total_events_cancelled,
        SUM(refund_amount) AS total_event_refunds
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
        SUM(push_sent) AS total_pushes_sent,
        COUNT(*) AS total_touchpoints
    FROM {{ ref('fact_marketing_touchpoints') }}
    GROUP BY contact_key
)

SELECT
    -- Identity
    c.contact_key,
    c.unified_contact_id,
    c.email,
    c.first_name,
    c.last_name,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.phone,

    -- Professional
    c.company_name,
    c.company_sector,
    c.company_sub_sector,
    c.company_business_model,
    c.company_employees,
    c.company_revenue,
    c.company_funding,
    c.company_stage,
    c.company_is_profitable,
    c.designation,
    c.seniority,
    c.linkedin_url,
    c.city,
    c.user_type,

    -- Plus membership
    c.plus_membership_type,
    c.plus_status,
    c.plus_days_to_expiry,
    CASE WHEN c.plus_status IN ('active', 'active_cancelling') THEN TRUE ELSE FALSE END AS is_plus_member,

    -- Orders
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    COALESCE(o.total_refunds, 0) AS total_refunds,
    COALESCE(o.net_ltv, 0) AS net_ltv,

    -- Events
    COALESCE(ev.total_events_registered, 0) AS total_events_registered,
    COALESCE(ev.total_events_active, 0) AS total_events_active,
    COALESCE(ev.total_events_cancelled, 0) AS total_events_cancelled,
    COALESCE(ev.total_event_refunds, 0) AS total_event_refunds,

    -- Forms
    COALESCE(f.total_form_submissions, 0) AS total_form_submissions,
    COALESCE(f.unique_forms_submitted, 0) AS unique_forms_submitted,

    -- Marketing
    COALESCE(m.total_emails_opened, 0) AS total_emails_opened,
    COALESCE(m.total_emails_clicked, 0) AS total_emails_clicked,
    COALESCE(m.total_unsubscribes, 0) AS total_unsubscribes,
    COALESCE(m.total_touchpoints, 0) AS total_touchpoints,

    -- Newsletters
    c.daily_newsletter,
    c.weekly_newsletter,
    c.ai_shift_newsletter,
    c.indepth_newsletter,
    c.theoutline_newsletter,
    c.markets_newsletter,
    (CASE WHEN c.daily_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.weekly_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.ai_shift_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.indepth_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.theoutline_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.markets_newsletter = 'subscribed' THEN 1 ELSE 0 END
    ) AS total_newsletters_subscribed,

    -- Lead
    c.lifecycle_stage,
    c.lead_status,
    c.hubspot_score,

    -- Engagement score (weighted composite)
    ROUND(
        COALESCE(c.sessions, 0) * 0.3
        + COALESCE(m.total_emails_opened, 0) * 2.0
        + COALESCE(m.total_emails_clicked, 0) * 5.0
        + COALESCE(ev.total_events_registered, 0) * 10.0
        + COALESCE(f.total_form_submissions, 0) * 8.0
        + COALESCE(o.total_orders, 0) * 15.0
        + CASE WHEN c.plus_status = 'active' THEN 20 ELSE 0 END
    , 1) AS engagement_score,

    -- Source coverage
    c.source_count,

    CURRENT_TIMESTAMP() AS updated_at

FROM contact c
LEFT JOIN orders o ON c.contact_key = o.contact_key
LEFT JOIN events ev ON c.contact_key = ev.contact_key
LEFT JOIN forms f ON c.contact_key = f.contact_key
LEFT JOIN marketing m ON c.contact_key = m.contact_key
