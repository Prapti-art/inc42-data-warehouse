-- Gold fact_event_attendance: one row per contact per event

WITH registrations AS (
    SELECT
        g.entry_id,
        LOWER(TRIM(COALESCE(g.email, g.work_email))) AS email,
        g.event_name,
        g.ticket_type AS registration_type,
        g.date_created AS registration_date,
        g.form_name,
        'gravity' AS source_system
    FROM `bigquery-296406`.`bronze`.`gravity_forms` g
    WHERE g.event_name IS NOT NULL
),

refunds AS (
    SELECT
        LOWER(TRIM(billing_email)) AS email,
        refund_amount,
        refund_reason,
        order_date
    FROM `bigquery-296406`.`bronze`.`woocommerce_orders`
    WHERE refund_amount > 0
)

SELECT
    ROW_NUMBER() OVER (ORDER BY r.entry_id) AS attendance_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(r.registration_date)) AS INT64) AS reg_date_key,
    r.event_name,
    r.registration_type,
    r.source_system,

    -- Registration status (check if refunded)
    CASE
        WHEN ref.refund_amount > 0 THEN 'cancelled'
        ELSE 'registered'
    END AS registration_status,

    -- Measures
    CASE WHEN r.registration_type = 'paid' THEN 1 ELSE 0 END AS is_paid,
    CASE WHEN ref.refund_amount > 0 THEN 1 ELSE 0 END AS cancelled_flag,
    COALESCE(ref.refund_amount, 0) AS refund_amount,
    ref.refund_reason AS cancellation_reason

FROM registrations r
LEFT JOIN `bigquery-296406`.`silver_gold`.`dim_contact` dc ON r.email = dc.email
LEFT JOIN refunds ref ON r.email = ref.email
    AND ABS(DATE_DIFF(DATE(r.registration_date), DATE(ref.order_date), DAY)) < 60