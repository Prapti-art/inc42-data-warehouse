-- Gold fact_event_attendance: one row per event/program registration
-- Source: fact_form_submissions where form_category is an event or program

SELECT
    submission_id AS registration_id,
    contact_key,
    submit_date_key AS reg_date_key,

    -- Event details
    form_name AS event_name,
    form_category AS event_category,
    form_subcategory AS event_subcategory,
    edition AS event_edition,
    source_system,

    -- Registration status (all form submissions = registered)
    'registered' AS registration_status,
    1 AS is_active,
    0 AS cancelled_flag,

    -- Is this a paid event?
    CASE
        WHEN form_category IN ('D2C Event', 'AI Workshop', 'GenAI Summit') THEN 1
        ELSE 0
    END AS is_paid_event,

    -- Event type classification
    CASE
        WHEN form_category IN ('D2C Event', 'AI Workshop', 'GenAI Summit', 'Webinar') THEN 'event'
        WHEN form_category IN ('FAST42', 'Startup Program', 'BrandLabs') THEN 'program'
        ELSE 'other'
    END AS event_type

FROM {{ ref('fact_form_submissions') }}
WHERE form_category IN (
    'D2C Event',
    'AI Workshop',
    'GenAI Summit',
    'Webinar',
    'FAST42',
    'Startup Program',
    'BrandLabs'
)
