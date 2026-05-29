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
        {{ event_role('event_name') }} AS event_role,
        FALSE AS is_paid,
        CAST(NULL AS STRING) AS pass_tier,
        CAST(NULL AS STRING) AS pass_type_original,
        CAST(0 AS NUMERIC) AS net_revenue,
        SAFE.PARSE_DATE('%Y%m%d', CAST(NULLIF(reg_date_key, 0) AS STRING)) AS interaction_date,
        'registered' AS attendance_status
    FROM {{ ref('fact_event_attendance') }}
    WHERE event_name IS NOT NULL AND TRIM(event_name) != ''
),

paid_tickets AS (
    -- NOTE: Standalone pass SKUs (Select/Enabler/Transfer/Investor/All Access/
    -- Startup Leaders Pass with no event prefix in product_name) are classified
    -- as D2C Summit in the event_franchise macro. These tier names are exclusive
    -- to D2C Summit at Inc42 — other summits use distinct pass naming.
    SELECT
        contact_key,
        product_name AS event_name_original,
        {{ event_franchise('product_name') }} AS event_franchise,
        COALESCE({{ event_format('product_name') }}, 'summit') AS event_format,
        'attendee' AS event_role,
        -- is_paid TRUE only for completed orders. Refunded = money returned, so
        -- treated as NOT paid (falls into the free bucket downstream); pending /
        -- cancelled never paid. attendance_status still distinguishes the four states.
        CASE WHEN is_completed = 1 THEN TRUE ELSE FALSE END AS is_paid,
        {{ pass_tier('pass_type') }} AS pass_tier,
        pass_type AS pass_type_original,
        CAST(COALESCE(net_revenue, 0) AS NUMERIC) AS net_revenue,
        SAFE.PARSE_DATE('%Y%m%d', CAST(NULLIF(order_date_key, 0) AS STRING)) AS interaction_date,
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

-- Derive event_edition from interaction_date year (single source of truth).
-- Free regs and paid tickets both happen in the year the event runs (or its
-- pre-event signup window), so interaction_date year is the reliable edition.
with_edition AS (
    SELECT *,
        CAST(EXTRACT(YEAR FROM interaction_date) AS STRING) AS event_edition
    FROM unioned
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
        ) AS rn,
        -- Grain-level revenue: sum ALL line items for this (contact, franchise,
        -- year, role), so a multi-pass / multi-ticket buyer is not undercounted
        -- by keeping only the surviving row's single net_revenue.
        SUM(CASE WHEN attendance_status = 'paid' THEN net_revenue ELSE 0 END) OVER (
            PARTITION BY contact_key, event_franchise, IFNULL(event_edition, ''), event_role
        ) AS grain_paid_revenue,
        SUM(CASE WHEN attendance_status = 'refunded' THEN net_revenue ELSE 0 END) OVER (
            PARTITION BY contact_key, event_franchise, IFNULL(event_edition, ''), event_role
        ) AS grain_refund_amount
    FROM with_edition
)

SELECT
    TO_HEX(MD5(CONCAT(
        CAST(d.contact_key AS STRING), '|',
        COALESCE(d.event_franchise, 'unknown'), '|',
        COALESCE(d.event_edition, ''), '|',
        COALESCE(d.event_role, 'registrant')
    ))) AS event_key,
    d.contact_key,
    c.unified_contact_id,
    c.email,
    d.event_franchise,
    d.event_edition,
    d.event_format,
    d.event_role,
    d.is_paid,
    d.pass_tier,
    d.pass_type_original,
    -- net_revenue is now the grain total (all completed line items for this
    -- contact×franchise×year×role), not just the surviving row's single value.
    d.grain_paid_revenue AS net_revenue,
    d.grain_refund_amount AS refund_amount,
    d.attendance_status,
    d.interaction_date,
    d.event_name_original,
    CURRENT_TIMESTAMP() AS updated_at
FROM deduped d
LEFT JOIN {{ ref('dim_contact') }} c USING (contact_key)
WHERE d.rn = 1
