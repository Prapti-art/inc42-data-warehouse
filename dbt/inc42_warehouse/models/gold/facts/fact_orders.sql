-- Gold fact_orders: one row per order item, linked to dim_contact via contact_key
-- Includes normalized product name, product type, and membership duration

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

-- Get product/event names from order items
order_items AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.order_item_name AS raw_product_name,
        oi.order_item_type
    FROM {{ source('bronze', 'woocommerce_order_items') }} oi
    WHERE oi.order_item_type = 'line_item'
),

-- Normalize product names
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
)

SELECT
    ni.order_item_id,
    o.order_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_date)) AS INT64) AS order_date_key,
    o.order_status,
    ni.raw_product_name, ni.product_name, ni.product_type, ni.membership_duration,
    o.order_total, o.tax_total, o.discount_amount,
    GREATEST(o.order_total - o.discount_amount, 0) AS net_revenue,
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
