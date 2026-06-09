-- Gold fact_orders: one row per order item, linked to dim_contact via contact_key.
-- Includes normalized product name, product type, membership duration, and
-- (for event tickets) pass_type + line-item refund_total.
--
-- Sources:
--   1. WooCommerce live  (bronze.woocommerce_orders + meta + items)
--   2. WooCommerce events historical backfill (bronze.woo_events_historical)
--      — D2C / GenAI / Fintech / Startup Leaders ticket sales, one-time CSV ingest

WITH order_meta_pivoted AS (
    SELECT
        m.post_id AS order_id,
        MAX(CASE WHEN m.meta_key = '_billing_email' THEN LOWER(TRIM(m.meta_value)) END) AS email,
        MAX(CASE WHEN m.meta_key = '_order_total' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS order_total,
        MAX(CASE WHEN m.meta_key = '_order_tax' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS tax_total,
        MAX(CASE WHEN m.meta_key = '_cart_discount' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS discount_amount,
        MAX(CASE WHEN m.meta_key = '_payment_method_title' THEN m.meta_value END) AS payment_method,
        MAX(CASE WHEN m.meta_key = '_order_currency' THEN m.meta_value END) AS currency,
        MAX(CASE WHEN m.meta_key = '_billing_company' THEN m.meta_value END) AS billing_company,
        MAX(CASE WHEN m.meta_key = '_billing_city' THEN m.meta_value END) AS billing_city,
        MAX(CASE WHEN m.meta_key = '_billing_state' THEN m.meta_value END) AS billing_state,
        MAX(CASE WHEN m.meta_key = '_billing_country' THEN m.meta_value END) AS billing_country,
        MAX(CASE WHEN m.meta_key = '_billing_gst' THEN m.meta_value END) AS billing_gst
    FROM {{ source('bronze', 'woocommerce_order_meta') }} m
    GROUP BY m.post_id
),

order_items AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.order_item_name AS raw_product_name,
        oi.order_item_type
    FROM {{ source('bronze', 'woocommerce_order_items') }} oi
    WHERE oi.order_item_type = 'line_item'
),

normalized_items AS (
    SELECT
        order_item_id,
        order_id,
        raw_product_name,
        CASE
            WHEN LOWER(raw_product_name) LIKE '%angelx%' THEN 'Inc42 Plus Angelx'
            WHEN LOWER(raw_product_name) LIKE '%inc42 plus%' OR raw_product_name = 'Inc42 Plus' THEN 'Inc42 Plus'
            WHEN LOWER(raw_product_name) LIKE '%datalabs pro contacts%' THEN 'Datalabs Pro Contacts'
            WHEN LOWER(raw_product_name) LIKE '%datalabs pro%' THEN 'Datalabs Pro'
            WHEN LOWER(raw_product_name) LIKE '%membership addon%' OR LOWER(raw_product_name) LIKE '%memebership addon%' THEN 'Membership Addon'
            WHEN LOWER(raw_product_name) LIKE '%contact addon%' THEN 'Contact Addon'
            WHEN LOWER(raw_product_name) LIKE '%credit topup%' THEN 'Credit Topup'
            WHEN LOWER(raw_product_name) LIKE '%test%' THEN 'Test'
            WHEN raw_product_name IS NULL OR TRIM(raw_product_name) = '' THEN 'Unknown'
            ELSE raw_product_name
        END AS product_name,
        CASE
            WHEN LOWER(raw_product_name) LIKE '%inc42 plus%' OR raw_product_name = 'Inc42 Plus' THEN 'membership'
            WHEN LOWER(raw_product_name) LIKE '%datalabs pro%' THEN 'membership'
            WHEN LOWER(raw_product_name) LIKE '%addon%' OR LOWER(raw_product_name) LIKE '%topup%' THEN 'addon'
            -- Event tickets (Apr 2026 — backfill via bronze.woo_events_historical)
            WHEN LOWER(raw_product_name) LIKE '%d2c summit%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%genai summit%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%fintech summit%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%startup leaders pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%enabler pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%investor pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%select pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%all access%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%growth pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%transfer pass%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%summit + workshop%' THEN 'event'
            WHEN LOWER(raw_product_name) LIKE '%test%' THEN 'test'
            WHEN raw_product_name IS NULL OR TRIM(raw_product_name) = '' THEN 'unknown'
            ELSE 'other'
        END AS product_type,
        CASE
            WHEN LOWER(raw_product_name) LIKE '%3 year%' THEN '3 Year'
            WHEN LOWER(raw_product_name) LIKE '%2 year%' THEN '2 Year'
            WHEN LOWER(raw_product_name) LIKE '%yearly%' OR LOWER(raw_product_name) LIKE '%annual%' THEN 'Annual'
            WHEN LOWER(raw_product_name) LIKE '%quarterly%' THEN 'Quarterly'
            WHEN LOWER(raw_product_name) LIKE '%monthly%' THEN 'Monthly'
            WHEN LOWER(raw_product_name) LIKE '%trial%' OR LOWER(raw_product_name) LIKE '%free%' THEN 'Trial'
            WHEN LOWER(raw_product_name) LIKE '%inc42 plus%' AND raw_product_name = 'Inc42 Plus' THEN 'Annual'
            ELSE NULL
        END AS membership_duration
    FROM order_items
),

orders AS (
    SELECT
        o.ID AS order_id,
        o.post_date AS order_date,
        o.post_status AS order_status,
        om.email,
        om.billing_company, om.billing_city, om.billing_state, om.billing_country, om.billing_gst,
        COALESCE(om.order_total, 0) AS order_total,
        COALESCE(om.tax_total, 0) AS tax_total,
        COALESCE(om.discount_amount, 0) AS discount_amount,
        om.payment_method, om.currency
    FROM {{ source('bronze', 'woocommerce_orders') }} o
    LEFT JOIN order_meta_pivoted om ON o.ID = om.order_id
),

-- Live WooCommerce orders (memberships, addons, anything not in historical events backfill)
live_orders AS (
    SELECT
        ni.order_item_id,
        o.order_id,
        dc.contact_key,
        CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_date)) AS INT64) AS order_date_key,
        o.order_status,
        ni.raw_product_name,
        ni.product_name,
        ni.product_type,
        ni.membership_duration,
        CAST(NULL AS STRING) AS pass_type,
        o.order_total, o.tax_total, o.discount_amount,
        GREATEST(o.order_total - o.discount_amount, 0) AS net_revenue,
        CAST(NULL AS NUMERIC) AS refund_total,
        o.payment_method, o.currency,
        o.billing_company, o.billing_city, o.billing_state, o.billing_country, o.billing_gst,
        CASE WHEN o.order_status = 'wc-completed' THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN o.order_status = 'wc-refunded' THEN 1 ELSE 0 END AS is_refunded,
        CASE WHEN o.order_status = 'wc-cancelled' THEN 1 ELSE 0 END AS is_cancelled,
        CASE WHEN o.order_status IN ('wc-processing', 'wc-completed') THEN 1 ELSE 0 END AS is_successful
    FROM normalized_items ni
    JOIN orders o ON ni.order_id = o.order_id
    LEFT JOIN {{ ref('dim_contact') }} dc ON o.email = dc.email
    WHERE ni.product_type != 'test'
      AND ni.product_name != 'Unknown'
      AND LOWER(COALESCE(o.email, '')) NOT LIKE '%@inc42.com'
      AND LOWER(COALESCE(o.email, '')) NOT LIKE '%test%'
),

-- Historical event-ticket orders (rich xlsx backfill — May 2026).
-- order_line_title is the proper event identifier (e.g., "Inc42 AI Summit 2026",
-- "The D2C Summit 3.0 - All Access Pass") and is preferred over product_name.
historical_events AS (
    SELECT
        CAST(NULL AS INT64) AS order_item_id,
        SAFE_CAST(h.order_id AS INT64) AS order_id,
        dc.contact_key,
        CAST(FORMAT_DATE('%Y%m%d', DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', h.order_date))) AS INT64) AS order_date_key,
        h.order_status,
        h.product_name AS raw_product_name,
        COALESCE(NULLIF(h.order_line_title, ''), h.product_name) AS product_name,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(COALESCE(h.order_line_title, h.product_name)),
                r'd2c summit|d2c day|d2c retreat|d2cx converge|genai summit|inc42 ai summit|ai summit by inc42|fintech summit|startup leaders pass|enabler pass|investor pass|select pass|all access|growth pass|transfer pass|summit \+ workshop|summit pass')
            THEN 'event'
            ELSE 'other'
        END AS product_type,
        CAST(NULL AS STRING) AS membership_duration,
        CAST(NULL AS STRING) AS pass_type,
        SAFE_CAST(h.order_total AS NUMERIC) AS order_total,
        CAST(NULL AS NUMERIC) AS tax_total,
        CAST(0 AS NUMERIC) AS discount_amount,
        GREATEST(SAFE_CAST(h.item_total AS NUMERIC), 0) AS net_revenue,
        COALESCE(SAFE_CAST(h.refund_total AS NUMERIC), 0) AS refund_total,
        h.payment_method, h.currency,
        h.billing_company, h.billing_city, h.billing_state, h.billing_country,
        CAST(NULL AS STRING) AS billing_gst,
        CASE WHEN h.order_status = 'wc-completed' THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN h.order_status = 'wc-refunded' OR SAFE_CAST(h.refund_total AS NUMERIC) > 0 THEN 1 ELSE 0 END AS is_refunded,
        CASE WHEN h.order_status = 'wc-cancelled' THEN 1 ELSE 0 END AS is_cancelled,
        CASE WHEN h.order_status IN ('wc-processing', 'wc-completed') THEN 1 ELSE 0 END AS is_successful
    FROM {{ source('bronze', 'woo_events_full') }} h
    LEFT JOIN {{ ref('dim_contact') }} dc ON LOWER(TRIM(h.billing_email)) = LOWER(TRIM(dc.email))
    WHERE LOWER(COALESCE(h.billing_email, '')) NOT LIKE '%@inc42.com'
      AND h.order_id IS NOT NULL
),

-- D2CX WooCommerce orders (CSV-loaded from d2cx.co's 3 separate WC stores).
-- These cover D2CX Foundations, D2CX AI, and D2CX Applications cohorts —
-- distinct franchises that don't exist in live_orders (inc42prod MySQL) or
-- historical_events (xlsx backfill). Dedup at silver via the standard
-- (contact, franchise, edition, role) ROW_NUMBER partition.
d2cx_wc_events AS (
    SELECT
        CAST(NULL AS INT64) AS order_item_id,
        SAFE_CAST(d.order_id AS INT64) AS order_id,
        dc.contact_key,
        CAST(FORMAT_DATE('%Y%m%d', DATE(d.order_date)) AS INT64) AS order_date_key,
        d.order_status,
        d.product_name AS raw_product_name,
        d.product_name,
        'event' AS product_type,
        CAST(NULL AS STRING) AS membership_duration,
        CAST(NULL AS STRING) AS pass_type,
        SAFE_CAST(d.order_total_inr AS NUMERIC) AS order_total,
        CAST(NULL AS NUMERIC) AS tax_total,
        CAST(0 AS NUMERIC) AS discount_amount,
        GREATEST(SAFE_CAST(d.item_total_inr AS NUMERIC), 0) AS net_revenue,
        CAST(0 AS NUMERIC) AS refund_total,
        CAST(NULL AS STRING) AS payment_method,
        'INR' AS currency,
        d.billing_company, d.billing_city, d.billing_state, d.billing_country,
        CAST(NULL AS STRING) AS billing_gst,
        -- D2CX stores sell digital goods only (event tickets, application slots).
        -- WC leaves paid digital orders at 'wc-processing' since there's no
        -- physical fulfillment step; treat both as completed for revenue/paid.
        CASE WHEN d.order_status IN ('wc-processing', 'wc-completed') THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN d.order_status = 'wc-refunded' THEN 1 ELSE 0 END AS is_refunded,
        CASE WHEN d.order_status = 'wc-cancelled' THEN 1 ELSE 0 END AS is_cancelled,
        CASE WHEN d.order_status IN ('wc-processing', 'wc-completed') THEN 1 ELSE 0 END AS is_successful
    FROM {{ source('bronze', 'd2cx_wc_orders') }} d
    LEFT JOIN {{ ref('dim_contact') }} dc ON LOWER(TRIM(d.billing_email)) = LOWER(TRIM(dc.email))
    WHERE d.order_id IS NOT NULL
      AND d.product_name IS NOT NULL
      AND LOWER(COALESCE(d.billing_email, '')) NOT LIKE '%@inc42.com'
)

SELECT * FROM live_orders
UNION ALL
SELECT * FROM historical_events
UNION ALL
SELECT * FROM d2cx_wc_events
