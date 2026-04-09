-- Gold fact_marketing_touchpoints: one row per email delivery metric
-- Source: Customer.io deliveries + metrics (sent, delivered, opened, clicked, bounced, unsubscribed)

WITH deliveries AS (
    SELECT
        d.delivery_id,
        d.internal_customer_id,
        d.delivery_type,
        d.campaign_id,
        d.newsletter_id,
        d.created_at AS delivery_date
    FROM {{ source('bronze', 'cio_deliveries') }} d
),

metrics AS (
    SELECT
        m.delivery_id,
        m.metric,
        m.created_at AS metric_date,
        m.link_url
    FROM {{ source('bronze', 'cio_metrics') }} m
),

campaigns AS (
    SELECT campaign_id, name AS campaign_name
    FROM {{ source('bronze', 'cio_campaigns') }} c
),

-- Map CIO internal_customer_id → unified_contact_id
contact_map AS (
    SELECT source_id, unified_contact_id
    FROM {{ source('silver', 'contact_source_xref') }}
    WHERE source_system = 'customerio'
),

-- Aggregate metrics per delivery
delivery_metrics AS (
    SELECT
        d.delivery_id,
        d.internal_customer_id,
        d.delivery_type,
        d.campaign_id,
        d.newsletter_id,
        d.delivery_date,
        MAX(CASE WHEN m.metric = 'sent' THEN 1 ELSE 0 END) AS sent,
        MAX(CASE WHEN m.metric = 'delivered' THEN 1 ELSE 0 END) AS delivered,
        MAX(CASE WHEN m.metric = 'opened' AND m.metric IS NOT NULL THEN 1 ELSE 0 END) AS opened,
        MAX(CASE WHEN m.metric = 'clicked' THEN 1 ELSE 0 END) AS clicked,
        MAX(CASE WHEN m.metric = 'bounced' THEN 1 ELSE 0 END) AS bounced,
        MAX(CASE WHEN m.metric = 'unsubscribed' THEN 1 ELSE 0 END) AS unsubscribed,
        MAX(CASE WHEN m.metric = 'dropped' THEN 1 ELSE 0 END) AS dropped
    FROM deliveries d
    LEFT JOIN metrics m ON d.delivery_id = m.delivery_id
    GROUP BY d.delivery_id, d.internal_customer_id, d.delivery_type,
             d.campaign_id, d.newsletter_id, d.delivery_date
)

SELECT
    dm.delivery_id AS touchpoint_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(dm.delivery_date)) AS INT64) AS activity_date_key,
    COALESCE(c.campaign_name, CAST(dm.newsletter_id AS STRING)) AS campaign_name,
    dm.delivery_type AS channel,

    -- Measures
    dm.sent,
    dm.delivered,
    dm.opened,
    dm.clicked,
    dm.bounced,
    dm.unsubscribed,
    dm.dropped

FROM delivery_metrics dm
LEFT JOIN contact_map cm ON dm.internal_customer_id = cm.source_id
LEFT JOIN {{ ref('dim_contact') }} dc ON cm.unified_contact_id = dc.unified_contact_id
LEFT JOIN campaigns c ON CAST(dm.campaign_id AS STRING) = CAST(c.campaign_id AS STRING)
