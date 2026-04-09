-- Gold fact_form_submissions: one row per form submission (Gravity Forms + Tally)

WITH gf_email_fields AS (
    -- Detect which field contains email per form
    SELECT form_id, meta_key AS email_field
    FROM (
        SELECT form_id, meta_key,
               COUNTIF(REGEXP_CONTAINS(meta_value, r'@[a-zA-Z0-9.-]+\.')) AS email_cnt,
               COUNT(*) AS total
        FROM {{ source('bronze', 'gravity_forms_entry_meta') }}
        WHERE SAFE_CAST(REGEXP_REPLACE(meta_key, r'\.', '') AS INT64) IS NOT NULL
        GROUP BY form_id, meta_key
        HAVING total > 50 AND SAFE_DIVIDE(email_cnt, total) > 0.5
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY form_id ORDER BY email_cnt DESC) = 1
),

gravity_forms AS (
    SELECT
        CAST(e.id AS STRING) AS submission_id,
        MAX(CASE WHEN m.meta_key = ef.email_field THEN LOWER(TRIM(m.meta_value)) END) AS email,
        CAST(e.form_id AS STRING) AS form_id,
        CAST(e.form_id AS STRING) AS form_name,  -- GF doesn't have form_name in entries
        e.date_created AS submit_date,
        'gravity_forms' AS source_system
    FROM {{ source('bronze', 'gravity_forms_entries') }} e
    JOIN gf_email_fields ef ON e.form_id = ef.form_id
    JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} m
        ON e.id = m.entry_id AND e.form_id = m.form_id
    WHERE e.status = 'active'
    GROUP BY e.id, e.form_id, e.date_created
),

tally_forms AS (
    SELECT
        response_id AS submission_id,
        LOWER(TRIM(COALESCE(email, work_email))) AS email,
        form_id,
        form_name,
        CAST(submitted_at AS STRING) AS submit_date,
        'tally' AS source_system
    FROM {{ source('bronze', 'tally_forms') }}
),

all_forms AS (
    SELECT * FROM gravity_forms
    UNION ALL
    SELECT * FROM tally_forms
)

SELECT
    f.submission_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(f.submit_date)) AS INT64) AS submit_date_key,
    f.form_id,
    f.form_name,
    f.source_system,
    1 AS submission_count

FROM all_forms f
LEFT JOIN {{ ref('dim_contact') }} dc ON f.email = dc.email
