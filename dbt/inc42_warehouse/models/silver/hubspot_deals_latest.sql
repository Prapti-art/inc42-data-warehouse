-- Silver: one clean row per HubSpot deal, with associated contact and
-- company IDs flattened out of the associations JSON blob.

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY modified_at DESC NULLS LAST, _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'hubspot_deals') }}
    WHERE id IS NOT NULL
),

deduped AS (
    SELECT * EXCEPT(rn)
    FROM latest
    WHERE rn = 1
      AND COALESCE(archived, FALSE) = FALSE
),

associations AS (
    SELECT
        id,
        ARRAY(
            SELECT JSON_EXTRACT_SCALAR(assoc, '$.id')
            FROM UNNEST(
                JSON_EXTRACT_ARRAY(
                    COALESCE(JSON_EXTRACT(associations_json, '$.contacts.results'), '[]')
                )
            ) AS assoc
        ) AS associated_contact_ids,
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

custom AS (
    SELECT
        id,
        SAFE_CAST(JSON_EXTRACT_SCALAR(properties_json, '$.hs_deal_stage_probability') AS NUMERIC) AS deal_stage_probability,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_deal_stage_probability_shadow') AS deal_stage_probability_shadow,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_analytics_source') AS analytics_source,
        JSON_EXTRACT_SCALAR(properties_json, '$.description') AS description,
        JSON_EXTRACT_SCALAR(properties_json, '$.hs_forecast_amount') AS forecast_amount
    FROM deduped
)

SELECT
    d.id                                           AS hubspot_deal_id,
    d.deal_name,
    SAFE_CAST(d.amount AS NUMERIC)                 AS amount_inr,
    d.pipeline,
    d.deal_stage,
    d.deal_type,
    COALESCE(SAFE_CAST(d.is_closed AS BOOL), d.is_closed = 'true') AS is_closed,
    COALESCE(SAFE_CAST(d.is_closed_won AS BOOL), d.is_closed_won = 'true') AS is_closed_won,

    c.deal_stage_probability,
    c.analytics_source,
    c.description,
    SAFE_CAST(c.forecast_amount AS NUMERIC)        AS forecast_amount_inr,

    d.owner_id                                     AS hubspot_owner_id,

    d.close_date,
    d.created_at                                   AS hubspot_created_at,
    d.modified_at                                  AS hubspot_modified_at,

    a.associated_contact_ids,
    a.associated_company_ids,

    d._ingested_at                                 AS _silver_built_at
FROM deduped d
LEFT JOIN associations a ON d.id = a.id
LEFT JOIN custom c       ON d.id = c.id
