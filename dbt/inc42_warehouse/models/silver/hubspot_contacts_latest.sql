-- Silver: one clean row per HubSpot contact.
-- Bronze is append-only (multiple rows per id across ingestion runs);
-- this view picks the latest version by modified_at, drops archived,
-- normalizes email/phone/linkedin, and pulls associated company IDs
-- out of the associations JSON blob.

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY modified_at DESC NULLS LAST, _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'hubspot_contacts') }}
    WHERE id IS NOT NULL
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM latest
    WHERE rn = 1
      AND COALESCE(archived, FALSE) = FALSE
),

-- Flatten associations_json → array of company IDs this contact is linked to.
-- HubSpot shape: {"companies": {"results": [{"id": "...", "type": "..."}]}}
associations AS (
    SELECT
        id,
        ARRAY(
            SELECT JSON_EXTRACT_SCALAR(assoc, '$.id')
            FROM UNNEST(
                JSON_EXTRACT_ARRAY(
                    COALESCE(JSON_EXTRACT(associations_json, '$.companies.results'), '[]')
                )
            ) AS assoc
        ) AS associated_company_ids
    FROM deduped
),

-- Pull any useful custom properties out of the full properties_json blob.
-- Anything not flat-column-ified by ingest still lives here.
custom AS (
    SELECT
        id,
        JSON_EXTRACT_SCALAR(properties_json, '$.unified_contact_id') AS unified_contact_id_hs,
        SAFE_CAST(JSON_EXTRACT_SCALAR(properties_json, '$.hubspotscore') AS NUMERIC) AS hubspot_score,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_analytics_source') AS analytics_source,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_analytics_source_data_1') AS analytics_source_detail,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_latest_source') AS latest_source
    FROM deduped
)

SELECT
    d.id                                           AS hubspot_contact_id,
    LOWER(TRIM(d.email))                           AS email,
    d.first_name,
    d.last_name,
    TRIM(CONCAT(COALESCE(d.first_name, ''), ' ', COALESCE(d.last_name, ''))) AS full_name,

    -- Phone — normalize to E.164 for Indian mobile; otherwise keep raw.
    d.phone                                        AS phone_raw,
    CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(d.phone, r'[\s\-\(\)\.\+]+', ''), r'^91[6-9]\d{9}$')
            THEN CONCAT('+', REGEXP_REPLACE(d.phone, r'[\s\-\(\)\.\+]+', ''))
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE(d.phone, r'[\s\-\(\)\.\+]+', ''), r'^[6-9]\d{9}$')
            THEN CONCAT('+91', REGEXP_REPLACE(d.phone, r'[\s\-\(\)\.\+]+', ''))
        ELSE NULL
    END                                            AS phone_e164,
    d.mobile_phone,

    -- LinkedIn — extract slug for matching (same rule as identity_resolution.py)
    d.linkedin_url,
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            LOWER(TRIM(d.linkedin_url)),
            r'https?://(www\.)?linkedin\.com/in/',
            ''
        ),
        r'[/?].*$',
        ''
    )                                              AS linkedin_slug,

    d.job_title,
    d.company_name,
    d.industry,
    d.city,
    d.state,
    d.country,

    -- Sales / lifecycle
    d.lifecycle_stage,
    d.lead_status,
    d.lead_source,
    c.hubspot_score,
    c.analytics_source,
    c.analytics_source_detail,
    c.latest_source,

    -- Ownership & cross-system keys
    d.owner_id                                     AS hubspot_owner_id,
    c.unified_contact_id_hs,                       -- value stamped in HubSpot (if any)
    a.associated_company_ids,

    -- Timestamps
    d.created_at                                   AS hubspot_created_at,
    d.modified_at                                  AS hubspot_modified_at,

    -- Audit
    d._ingested_at                                 AS _silver_built_at
FROM deduped d
LEFT JOIN associations a ON d.id = a.id
LEFT JOIN custom c       ON d.id = c.id
