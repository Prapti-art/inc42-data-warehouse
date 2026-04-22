-- Gold fact: one row per HubSpot deal with the primary associated contact
-- and company resolved into contact_key / company_key for BI joins.
--
-- A deal can have multiple associated contacts/companies in HubSpot; we pick
-- the first one as "primary" for the flattened fact row. Raw arrays are kept
-- for deeper analysis.

WITH deals AS (
    SELECT * FROM {{ ref('hubspot_deals_latest') }}
),

-- Pick the first associated contact/company per deal.
primary_assoc AS (
    SELECT
        hubspot_deal_id,
        associated_contact_ids[SAFE_OFFSET(0)] AS primary_hubspot_contact_id,
        associated_company_ids[SAFE_OFFSET(0)] AS primary_hubspot_company_id
    FROM deals
),

-- Use primary_email to link HubSpot contact → dim_contact, via silver.hubspot_contacts_latest
contact_link AS (
    SELECT
        hc.hubspot_contact_id,
        dc.contact_key
    FROM {{ ref('hubspot_contacts_latest') }} hc
    LEFT JOIN {{ ref('dim_contact') }} dc ON LOWER(TRIM(hc.email)) = LOWER(TRIM(dc.email))
),

-- Link HubSpot company → dim_company by normalized name match
company_link AS (
    SELECT
        hco.hubspot_company_id,
        dco.company_key
    FROM {{ ref('hubspot_companies_latest') }} hco
    LEFT JOIN {{ ref('dim_company') }} dco ON hco.name_lower = LOWER(TRIM(dco.company_name))
)

SELECT
    d.hubspot_deal_id,
    d.deal_name,
    d.amount_inr,
    d.pipeline,
    d.deal_stage,
    d.deal_type,
    d.is_closed,
    d.is_closed_won,
    d.deal_stage_probability,
    d.forecast_amount_inr,
    d.description,
    d.analytics_source,

    -- Primary contact resolution
    pa.primary_hubspot_contact_id,
    cl.contact_key AS primary_contact_key,

    -- Primary company resolution
    pa.primary_hubspot_company_id,
    colink.company_key AS primary_company_key,

    -- All associations preserved for analytics
    d.associated_contact_ids,
    d.associated_company_ids,

    d.hubspot_owner_id,
    d.close_date,
    d.hubspot_created_at,
    d.hubspot_modified_at,

    CURRENT_TIMESTAMP() AS updated_at
FROM deals d
LEFT JOIN primary_assoc pa ON d.hubspot_deal_id = pa.hubspot_deal_id
LEFT JOIN contact_link cl  ON pa.primary_hubspot_contact_id = cl.hubspot_contact_id
LEFT JOIN company_link colink ON pa.primary_hubspot_company_id = colink.hubspot_company_id
