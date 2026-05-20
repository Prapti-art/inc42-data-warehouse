-- silver.events: ONE ROW per (contact, event_franchise, edition, role).
-- Single source of truth for all event interactions: form registrations
-- (fact_event_attendance), paid tickets (fact_orders w/ product_type='event'),
-- and paid memberships at summits (membership flagged separately upstream).
--
-- Decisions: D2CX Converge is its own franchise (not under D2C Summit);
-- BigShift is its own franchise (not under FAST42).

WITH free_registrations AS (
    SELECT
        contact_key,
        event_name AS event_name_original,
        {{ event_franchise('event_name') }} AS event_franchise,
        {{ event_format('event_name') }} AS event_format,
        {{ event_edition('event_name') }} AS event_edition,
        {{ event_role('event_name') }} AS event_role,
        FALSE AS is_paid,
        CAST(NULL AS STRING) AS pass_tier,
        CAST(NULL AS STRING) AS pass_type_original,
        CAST(0 AS NUMERIC) AS net_revenue,
        SAFE.PARSE_DATE('%Y%m%d', CAST(NULLIF(reg_date_key, 0) AS STRING)) AS interaction_date,
        'form_registration' AS source_type,
        'registered' AS attendance_status
    FROM {{ ref('fact_event_attendance') }}
    WHERE event_name IS NOT NULL AND TRIM(event_name) != ''
),

paid_tickets AS (
    SELECT
        contact_key,
        product_name AS event_name_original,
        {{ event_franchise('product_name') }} AS event_franchise,
        COALESCE({{ event_format('product_name') }}, 'summit') AS event_format,
        -- Fall back to order-year when product_name has no edition (pass-only SKUs)
        COALESCE(
            {{ event_edition('product_name') }},
            CAST(EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y%m%d', CAST(NULLIF(order_date_key, 0) AS STRING))) AS STRING)
        ) AS event_edition,
        'attendee' AS event_role,
        TRUE AS is_paid,
        {{ pass_tier('pass_type') }} AS pass_tier,
        pass_type AS pass_type_original,
        CAST(COALESCE(net_revenue, 0) AS NUMERIC) AS net_revenue,
        SAFE.PARSE_DATE('%Y%m%d', CAST(NULLIF(order_date_key, 0) AS STRING)) AS interaction_date,
        'paid_ticket' AS source_type,
        CASE
            WHEN is_completed = 1 THEN 'paid'
            WHEN is_refunded = 1 THEN 'refunded'
            WHEN is_cancelled = 1 THEN 'cancelled'
            ELSE 'pending'
        END AS attendance_status
    FROM {{ ref('fact_orders') }}
    WHERE product_type = 'event' AND product_name IS NOT NULL
),

unioned AS (
    SELECT * FROM free_registrations
    UNION ALL
    SELECT * FROM paid_tickets
),

-- One row per (contact, franchise, edition, role).
-- Prefer paid over free; within paid, prefer All Access > Growth > Live > Select > Enabler.
-- Within free, prefer most recent.
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY contact_key, event_franchise, IFNULL(event_edition, ''), event_role
            ORDER BY
                is_paid DESC,
                CASE pass_tier
                    WHEN 'All Access' THEN 1
                    WHEN 'Growth' THEN 2
                    WHEN 'Live' THEN 3
                    WHEN 'Select' THEN 4
                    WHEN 'Enabler' THEN 5
                    ELSE 9
                END,
                interaction_date DESC NULLS LAST
        ) AS rn
    FROM unioned
)

SELECT
    TO_HEX(MD5(CONCAT(
        CAST(contact_key AS STRING), '|',
        COALESCE(event_franchise, 'unknown'), '|',
        COALESCE(event_edition, ''), '|',
        COALESCE(event_role, 'registrant')
    ))) AS event_key,
    contact_key,
    event_franchise,
    event_edition,
    event_format,
    event_role,
    is_paid,
    pass_tier,
    pass_type_original,
    net_revenue,
    attendance_status,
    interaction_date,
    source_type,
    event_name_original,
    CURRENT_TIMESTAMP() AS updated_at
FROM deduped
WHERE rn = 1
