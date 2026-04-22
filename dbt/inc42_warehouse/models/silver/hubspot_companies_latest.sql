-- Silver: one clean row per HubSpot company.

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY modified_at DESC NULLS LAST, _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'hubspot_companies') }}
    WHERE id IS NOT NULL
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM latest
    WHERE rn = 1
      AND COALESCE(archived, FALSE) = FALSE
),

custom AS (
    SELECT
        id,
        JSON_EXTRACT_SCALAR(properties_json, '$.description') AS description,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_analytics_source') AS analytics_source,
        JSON_EXTRACT_SCALAR(properties_json, '$.phone') AS phone,
        JSON_EXTRACT_SCALAR(properties_json, '$.linkedin_company_page') AS linkedin_url,
        JSON_EXTRACT_SCALAR(properties_json, '$.twitterhandle') AS twitter_handle
    FROM deduped
)

SELECT
    d.id                                           AS hubspot_company_id,
    TRIM(d.name)                                   AS name,
    LOWER(TRIM(d.name))                            AS name_lower,
    LOWER(TRIM(d.domain))                          AS domain,
    d.industry,
    SAFE_CAST(d.employee_count AS INT64)           AS employee_count,
    SAFE_CAST(d.annual_revenue AS NUMERIC)         AS annual_revenue,
    d.city,
    d.state,
    d.country,
    SAFE_CAST(d.founded_year AS INT64)             AS founded_year,
    d.lifecycle_stage,
    d.type                                         AS company_type,

    c.description,
    c.analytics_source,
    c.phone,
    c.linkedin_url,
    c.twitter_handle,

    d.owner_id                                     AS hubspot_owner_id,

    d.created_at                                   AS hubspot_created_at,
    d.modified_at                                  AS hubspot_modified_at,

    d._ingested_at                                 AS _silver_built_at
FROM deduped d
LEFT JOIN custom c ON d.id = c.id
