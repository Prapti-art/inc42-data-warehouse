-- Gold fact_orders: one row per order, linked to dimensions via keys

WITH orders AS (
    SELECT
        o.order_id,
        o.order_date,
        o.order_status,
        o.order_total,
        o.tax_total,
        o.discount_amount,
        o.coupon_code,
        o.payment_method,
        o.refund_amount,
        o.refund_reason,
        LOWER(TRIM(o.billing_email)) AS email,
        o.billing_city,
        o.billing_state,
        o.billing_country
    FROM `bigquery-296406`.`bronze`.`woocommerce_orders` o
)

SELECT
    o.order_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_date)) AS INT64) AS order_date_key,
    o.order_status,
    o.order_total,
    o.tax_total,
    o.discount_amount,
    o.order_total - COALESCE(o.discount_amount, 0) - COALESCE(o.tax_total, 0) AS net_revenue,
    COALESCE(o.refund_amount, 0) AS refund_amount,
    o.refund_reason,
    o.coupon_code,
    o.payment_method,
    o.billing_city,
    o.billing_state,
    o.billing_country,
    -- Flags
    CASE WHEN o.order_status = 'completed' THEN 1 ELSE 0 END AS is_completed,
    CASE WHEN o.order_status = 'refunded' THEN 1 ELSE 0 END AS is_refunded,
    CASE WHEN o.coupon_code IS NOT NULL THEN 1 ELSE 0 END AS used_coupon

FROM orders o
LEFT JOIN `bigquery-296406`.`silver_gold`.`dim_contact` dc
    ON o.email = dc.email