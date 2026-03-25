-- Gold fact_form_submissions: one row per form submission

WITH gravity_forms AS (
    SELECT
        CAST(entry_id AS STRING) AS submission_id,
        LOWER(TRIM(COALESCE(email, work_email))) AS email,
        CAST(form_id AS STRING) AS form_id,
        form_name,
        date_created AS submit_date,
        'gravity' AS source_system
    FROM {{ source('bronze', 'gravity_forms') }}
),

tally_forms AS (
    SELECT
        response_id AS submission_id,
        LOWER(TRIM(COALESCE(email, work_email))) AS email,
        form_id,
        form_name,
        submitted_at AS submit_date,
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
    1 AS submission_count  -- always 1, SUM for totals

FROM all_forms f
LEFT JOIN {{ ref('dim_contact') }} dc ON f.email = dc.email
