-- Gold fact_event_attendance: one row per event registration
-- Events come from WooCommerce orders (paid events/workshops/tickets)

WITH order_meta AS (
    SELECT
        m.order_id,
        MAX(CASE WHEN m.meta_key = '_billing_email' THEN LOWER(TRIM(m.meta_value)) END) AS email
    FROM {{ source('bronze', 'woocommerce_order_meta') }} m
    WHERE m.meta_key = '_billing_email'
    GROUP BY m.order_id
),

-- Get product names from order items
order_items AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.order_item_name AS event_name,
        oi.order_item_type
    FROM {{ source('bronze', 'woocommerce_order_items') }} oi
    WHERE oi.order_item_type = 'line_item'
),

registrations AS (
    SELECT
        oi.order_item_id AS registration_id,
        om.email,
        oi.event_name,
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
    r.event_name,
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
