-- Gold fact_orders: one row per order, linked to dim_contact via contact_key

WITH order_meta_pivoted AS (
    SELECT
        m.order_id,
        MAX(CASE WHEN m.meta_key = '_billing_email' THEN LOWER(TRIM(m.meta_value)) END) AS email,
        MAX(CASE WHEN m.meta_key = '_billing_first_name' THEN m.meta_value END) AS billing_first_name,
        MAX(CASE WHEN m.meta_key = '_billing_last_name' THEN m.meta_value END) AS billing_last_name,
        MAX(CASE WHEN m.meta_key = '_billing_phone' THEN m.meta_value END) AS billing_phone,
        MAX(CASE WHEN m.meta_key = '_billing_company' THEN m.meta_value END) AS billing_company,
        MAX(CASE WHEN m.meta_key = '_billing_city' THEN m.meta_value END) AS billing_city,
        MAX(CASE WHEN m.meta_key = '_billing_state' THEN m.meta_value END) AS billing_state,
        MAX(CASE WHEN m.meta_key = '_billing_country' THEN m.meta_value END) AS billing_country,
        MAX(CASE WHEN m.meta_key = '_order_total' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS order_total,
        MAX(CASE WHEN m.meta_key = '_order_tax' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS tax_total,
        MAX(CASE WHEN m.meta_key = '_cart_discount' THEN SAFE_CAST(m.meta_value AS NUMERIC) END) AS discount_amount,
        MAX(CASE WHEN m.meta_key = '_payment_method_title' THEN m.meta_value END) AS payment_method,
        MAX(CASE WHEN m.meta_key = '_order_currency' THEN m.meta_value END) AS currency,
        MAX(CASE WHEN m.meta_key = '_billing_gst' THEN m.meta_value END) AS billing_gst
    FROM {{ source('bronze', 'woocommerce_order_meta') }} m
    GROUP BY m.order_id
),

orders AS (
    SELECT
        o.order_id,
        o.post_date AS order_date,
        o.order_status,
        om.email,
        om.billing_first_name,
        om.billing_last_name,
        om.billing_phone,
        om.billing_company,
        om.billing_city,
        om.billing_state,
        om.billing_country,
        COALESCE(om.order_total, 0) AS order_total,
        COALESCE(om.tax_total, 0) AS tax_total,
        COALESCE(om.discount_amount, 0) AS discount_amount,
        om.payment_method,
        om.currency,
        om.billing_gst
    FROM {{ source('bronze', 'woocommerce_orders') }} o
    LEFT JOIN order_meta_pivoted om ON o.order_id = om.order_id
)

SELECT
    o.order_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_date)) AS INT64) AS order_date_key,
    o.order_status,
    o.order_total,
    o.tax_total,
    o.discount_amount,
    o.order_total - o.discount_amount AS net_revenue,
    o.payment_method,
    o.currency,
    o.billing_company,
    o.billing_city,
    o.billing_state,
    o.billing_country,
    o.billing_gst,

    -- Flags
    CASE WHEN o.order_status = 'wc-completed' THEN 1 ELSE 0 END AS is_completed,
    CASE WHEN o.order_status = 'wc-refunded' THEN 1 ELSE 0 END AS is_refunded,
    CASE WHEN o.order_status = 'wc-cancelled' THEN 1 ELSE 0 END AS is_cancelled,
    CASE WHEN o.order_status IN ('wc-processing', 'wc-completed') THEN 1 ELSE 0 END AS is_successful

FROM orders o
LEFT JOIN {{ ref('dim_contact') }} dc ON o.email = dc.email
