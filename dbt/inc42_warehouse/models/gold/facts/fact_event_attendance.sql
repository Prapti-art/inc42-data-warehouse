-- Gold fact_event_attendance: one row per order item (product/membership/event)
-- Normalized product names, membership types, and durations

WITH order_meta AS (
    SELECT
        m.order_id,
        MAX(CASE WHEN m.meta_key = '_billing_email' THEN LOWER(TRIM(m.meta_value)) END) AS email
    FROM {{ source('bronze', 'woocommerce_order_meta') }} m
    WHERE m.meta_key = '_billing_email'
    GROUP BY m.order_id
),

order_items AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.order_item_name AS raw_name,
        oi.order_item_type
    FROM {{ source('bronze', 'woocommerce_order_items') }} oi
    WHERE oi.order_item_type = 'line_item'
),

registrations AS (
    SELECT
        oi.order_item_id AS registration_id,
        om.email,
        oi.raw_name,

        -- Normalized event/product name
        CASE
            WHEN LOWER(oi.raw_name) LIKE '%angelx%' THEN 'Inc42 Plus Angelx'
            WHEN LOWER(oi.raw_name) LIKE '%inc42 plus%' OR oi.raw_name = 'Inc42 Plus' THEN 'Inc42 Plus'
            WHEN LOWER(oi.raw_name) LIKE '%datalabs pro contacts%' THEN 'Datalabs Pro Contacts'
            WHEN LOWER(oi.raw_name) LIKE '%datalabs pro%' THEN 'Datalabs Pro'
            WHEN LOWER(oi.raw_name) LIKE '%membership addon%' OR LOWER(oi.raw_name) LIKE '%memebership addon%' THEN 'Membership Addon'
            WHEN LOWER(oi.raw_name) LIKE '%contact addon%' THEN 'Contact Addon'
            WHEN LOWER(oi.raw_name) LIKE '%credit topup%' THEN 'Credit Topup'
            WHEN LOWER(oi.raw_name) LIKE '%test%' THEN 'Test'
            WHEN oi.raw_name IS NULL OR TRIM(oi.raw_name) = '' THEN 'Unknown'
            ELSE oi.raw_name
        END AS event_name,

        -- Product type
        CASE
            WHEN LOWER(oi.raw_name) LIKE '%inc42 plus%' OR oi.raw_name = 'Inc42 Plus' THEN 'membership'
            WHEN LOWER(oi.raw_name) LIKE '%datalabs pro%' THEN 'membership'
            WHEN LOWER(oi.raw_name) LIKE '%addon%' OR LOWER(oi.raw_name) LIKE '%topup%' THEN 'addon'
            WHEN LOWER(oi.raw_name) LIKE '%test%' THEN 'test'
            WHEN oi.raw_name IS NULL OR TRIM(oi.raw_name) = '' THEN 'unknown'
            ELSE 'other'
        END AS product_type,

        -- Membership duration
        CASE
            WHEN LOWER(oi.raw_name) LIKE '%3 year%' THEN '3 Year'
            WHEN LOWER(oi.raw_name) LIKE '%2 year%' THEN '2 Year'
            WHEN LOWER(oi.raw_name) LIKE '%yearly%' OR LOWER(oi.raw_name) LIKE '%annual%' THEN 'Annual'
            WHEN LOWER(oi.raw_name) LIKE '%quarterly%' THEN 'Quarterly'
            WHEN LOWER(oi.raw_name) LIKE '%monthly%' THEN 'Monthly'
            WHEN LOWER(oi.raw_name) LIKE '%trial%' OR LOWER(oi.raw_name) LIKE '%free%' THEN 'Trial'
            WHEN LOWER(oi.raw_name) LIKE '%inc42 plus%' AND oi.raw_name = 'Inc42 Plus' THEN 'Annual'
            ELSE NULL
        END AS membership_duration,

        o.post_date AS registration_date,
        o.order_status,
        'woocommerce' AS source_system
    FROM {{ source('bronze', 'woocommerce_orders') }} o
    JOIN order_meta om ON o.order_id = om.order_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE om.email IS NOT NULL
)

SELECT
    r.registration_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(r.registration_date)) AS INT64) AS reg_date_key,

    -- Product/Event details
    r.raw_name,
    r.event_name,
    r.product_type,
    r.membership_duration,
    r.source_system,

    -- Registration status based on WooCommerce order status
    CASE
        WHEN r.order_status = 'wc-refunded' THEN 'cancelled'
        WHEN r.order_status = 'wc-cancelled' THEN 'cancelled'
        WHEN r.order_status IN ('wc-completed', 'wc-processing') THEN 'registered'
        ELSE r.order_status
    END AS registration_status,

    -- Flags
    CASE WHEN r.order_status IN ('wc-completed', 'wc-processing') THEN 1 ELSE 0 END AS is_active,
    CASE WHEN r.order_status IN ('wc-refunded', 'wc-cancelled') THEN 1 ELSE 0 END AS cancelled_flag,
    1 AS is_paid

FROM registrations r
LEFT JOIN {{ ref('dim_contact') }} dc ON r.email = dc.email
WHERE r.product_type != 'test'                         -- exclude test products
  AND r.event_name != 'Unknown'                          -- exclude empty line items
  AND LOWER(COALESCE(r.email, '')) NOT LIKE '%@inc42.com' -- exclude Inc42 internal
  AND LOWER(COALESCE(r.email, '')) NOT LIKE '%test%'      -- exclude test emails
