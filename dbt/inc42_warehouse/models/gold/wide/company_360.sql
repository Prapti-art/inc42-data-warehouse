-- Gold company_360: ONE ROW = EVERYTHING about a company
-- Datalabs company data + Inc42 engagement + sector thesis + Inc42 tags + investors

WITH company AS (
    SELECT * FROM {{ ref('dim_company') }}
),

-- Inc42 sector thesis (proper sector classification)
sector_thesis AS (
    SELECT
        st.company_uuid,
        STRING_AGG(DISTINCT th.sector, ', ') AS inc42_sectors,
        STRING_AGG(DISTINCT th.sub_sector, ', ') AS inc42_sub_sectors
    FROM {{ source('bronze', 'dl_sector_tagging') }} st
    JOIN {{ source('bronze', 'dl_inc42_sector_thesis') }} th ON st.tag_id = th.tag_id
    GROUP BY st.company_uuid
),

-- Inc42 company tags (Unicorn, Soonicorn, FAST42, 30 Startups To Watch, etc.)
inc42_tags AS (
    SELECT
        company_uuid,
        STRING_AGG(DISTINCT CONCAT(tag_name, ' (', category, ')'), ', ') AS inc42_tags,
        MAX(CASE WHEN tag_name = 'Unicorn' THEN TRUE ELSE FALSE END) AS is_unicorn,
        MAX(CASE WHEN tag_name = 'Soonicorn' THEN TRUE ELSE FALSE END) AS is_soonicorn,
        MAX(CASE WHEN tag_name = 'Minicorn' THEN TRUE ELSE FALSE END) AS is_minicorn,
        MAX(CASE WHEN tag_name = 'FAST42' THEN TRUE ELSE FALSE END) AS is_fast42_winner,
        MAX(CASE WHEN tag_name = '30 Startups To Watch' THEN TRUE ELSE FALSE END) AS is_30_startups_to_watch,
        MAX(CASE WHEN tag_name = 'Startup Watchlist' THEN TRUE ELSE FALSE END) AS is_startup_watchlist,
        MAX(CASE WHEN tag_name = 'Inc42 UpNext' THEN TRUE ELSE FALSE END) AS is_inc42_upnext,
        MAX(CASE WHEN category = 'Tracker' THEN TRUE ELSE FALSE END) AS is_tracked
    FROM {{ source('bronze', 'dl_inc42_company_tag') }}
    WHERE status = 'active'
    GROUP BY company_uuid
),

-- Company investors
investors AS (
    SELECT
        f.company_uuid,
        STRING_AGG(DISTINCT inv.name, ', ') AS investors,
        COUNT(DISTINCT inv.investor_uuid) AS investor_count
    FROM {{ source('bronze', 'dl_funding_table') }} f
    JOIN {{ source('bronze', 'dl_investment_table') }} i ON CAST(f.funding_uuid AS STRING) = CAST(i.funding_uuid AS STRING)
    JOIN {{ source('bronze', 'dl_investor_table') }} inv ON i.investor_uuid = inv.investor_uuid
    GROUP BY f.company_uuid
),

-- How many Inc42 contacts work at each company
inc42_contacts AS (
    SELECT
        LOWER(TRIM(company_name)) AS company_name_lower,
        COUNT(*) AS contacts_in_warehouse,
        COUNT(DISTINCT contact_key) AS unique_contacts
    FROM {{ ref('dim_contact') }}
    WHERE company_name IS NOT NULL AND company_name != ''
    GROUP BY LOWER(TRIM(company_name))
),

-- Revenue from WooCommerce orders linked to company employees
company_orders AS (
    SELECT
        LOWER(TRIM(dc.company_name)) AS company_name_lower,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN fo.is_completed = 1 THEN fo.net_revenue ELSE 0 END) AS total_revenue
    FROM {{ ref('fact_orders') }} fo
    JOIN {{ ref('dim_contact') }} dc ON fo.contact_key = dc.contact_key
    WHERE dc.company_name IS NOT NULL AND dc.company_name != ''
    GROUP BY LOWER(TRIM(dc.company_name))
)

SELECT
    co.company_key,
    co.company_id,
    co.company_name,
    co.website,
    co.domain,

    -- Sector (from Inc42 sector thesis — proper classification)
    sth.inc42_sectors AS sector,
    sth.inc42_sub_sectors AS sub_sectors,
    co.sector AS dl_sector_raw,
    co.sub_sector AS dl_sub_sector_raw,
    co.business_model,

    -- Location
    co.city,
    co.state,
    co.country,
    co.founded_year,
    co.tags,
    co.linkedin,

    -- Funding
    co.total_funding_usd,
    co.last_funding_date,
    co.last_funding_stage,
    co.last_funding_type,
    co.total_funding_rounds,

    -- Investors
    inv.investors,
    COALESCE(inv.investor_count, 0) AS investor_count,

    -- Financials
    co.revenue_from_operations,
    co.total_revenue,
    co.profit_loss_for_the_period,
    co.is_profitable,

    -- Employees & Web
    co.employee_count,
    co.monthly_visits,

    -- Inc42 tags
    it.inc42_tags,
    COALESCE(it.is_unicorn, FALSE) AS is_unicorn,
    COALESCE(it.is_soonicorn, FALSE) AS is_soonicorn,
    COALESCE(it.is_minicorn, FALSE) AS is_minicorn,
    COALESCE(it.is_fast42_winner, FALSE) AS is_fast42_winner,
    COALESCE(it.is_30_startups_to_watch, FALSE) AS is_30_startups_to_watch,
    COALESCE(it.is_startup_watchlist, FALSE) AS is_startup_watchlist,
    COALESCE(it.is_inc42_upnext, FALSE) AS is_inc42_upnext,
    COALESCE(it.is_tracked, FALSE) AS is_tracked,

    -- Inc42 engagement
    COALESCE(ic.contacts_in_warehouse, 0) AS contacts_in_warehouse,
    COALESCE(ic.unique_contacts, 0) AS unique_contacts,
    COALESCE(cor.total_orders, 0) AS total_orders_from_company,
    COALESCE(cor.total_revenue, 0) AS total_revenue_from_company,

    -- HubSpot (sales pipeline, matched by normalized name)
    hco.hubspot_company_id,
    hco.lifecycle_stage AS hubspot_lifecycle_stage,
    hco.company_type AS hubspot_company_type,
    hco.hubspot_owner_id,
    hco.hubspot_created_at AS hubspot_created_at,
    hco.hubspot_modified_at AS hubspot_modified_at,

    CURRENT_TIMESTAMP() AS updated_at

FROM company co
LEFT JOIN sector_thesis sth ON co.company_id = sth.company_uuid
LEFT JOIN inc42_tags it ON co.company_id = it.company_uuid
LEFT JOIN investors inv ON co.company_id = inv.company_uuid
LEFT JOIN inc42_contacts ic ON LOWER(TRIM(co.company_name)) = ic.company_name_lower
LEFT JOIN company_orders cor ON LOWER(TRIM(co.company_name)) = cor.company_name_lower
LEFT JOIN {{ ref('hubspot_companies_latest') }} hco ON LOWER(TRIM(co.company_name)) = hco.name_lower
