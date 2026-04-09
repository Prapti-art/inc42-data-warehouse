-- Gold contact_360: ONE ROW = EVERYTHING about a person
-- Enriched with: company data (Datalabs), people data (LinkedIn), investments, Inc42 tags, sector thesis

WITH contact AS (
    SELECT * FROM {{ ref('dim_contact') }}
),

-- ═══════════════════════════════════════════════
-- COMPANY ENRICHMENT (via company name match)
-- ═══════════════════════════════════════════════
company_match AS (
    SELECT company_name_lower, company_uuid,
           dl_sub_sector, dl_business_model,
           dl_company_city, dl_company_state, dl_company_country,
           dl_founded_year, dl_company_website, dl_company_linkedin, dl_company_tags
    FROM (
        SELECT
            LOWER(TRIM(ct.name)) AS company_name_lower,
            ct.company_uuid,
            ct.sub_sector AS dl_sub_sector,
            ct.business_model AS dl_business_model,
            ct.city AS dl_company_city,
            ct.state AS dl_company_state,
            ct.country AS dl_company_country,
            ct.founded_date AS dl_founded_year,
            ct.website AS dl_company_website,
            ct.linkedin AS dl_company_linkedin,
            ct.tags AS dl_company_tags,
            ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(ct.name)) ORDER BY ct.last_modified_date DESC) AS rn
        FROM {{ source('bronze', 'dl_company_table') }} ct
        WHERE ct.name IS NOT NULL
    )
    WHERE rn = 1
),

-- Company funding summary
company_funding AS (
    SELECT
        company_uuid,
        SUM(amount_raised_in_usd) AS total_funding_usd,
        MAX(funding_date) AS last_funding_date,
        MAX(CASE WHEN funding_date = max_date THEN funding_stage END) AS last_funding_stage,
        COUNT(*) AS total_funding_rounds
    FROM (
        SELECT *, MAX(funding_date) OVER (PARTITION BY company_uuid) AS max_date
        FROM {{ source('bronze', 'dl_funding_table') }}
        WHERE amount_raised_in_usd IS NOT NULL
    )
    GROUP BY company_uuid
),

-- Company financials (latest P&L, prefer Consolidated)
company_financials AS (
    SELECT company_uuid, revenue_from_operations, total_revenue,
           profit_loss_for_the_period, total_expenses,
           CASE WHEN profit_loss_for_the_period > 0 THEN TRUE ELSE FALSE END AS is_profitable
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY company_uuid
                ORDER BY CASE WHEN financial_type = 'Consolidated' THEN 0 ELSE 1 END, as_of_date DESC
            ) AS rn
        FROM {{ source('bronze', 'dl_profit_loss_table') }}
    )
    WHERE rn = 1
),

-- Company employee count (latest)
company_employees AS (
    SELECT company_uuid, employee_count_number AS employee_count
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY company_uuid ORDER BY as_of_date DESC) AS rn
        FROM {{ source('bronze', 'dl_employee_trendline') }}
        WHERE employee_count_number IS NOT NULL
    )
    WHERE rn = 1
),

-- Company web traffic (latest)
company_web AS (
    SELECT company_uuid, monthly_visits
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY company_uuid ORDER BY as_of_date DESC) AS rn
        FROM {{ source('bronze', 'dl_website_analytics_table') }}
    )
    WHERE rn = 1
),

-- Company investors (who funded this company)
company_investors AS (
    SELECT
        f.company_uuid,
        STRING_AGG(DISTINCT inv.name, ', ') AS investors,
        COUNT(DISTINCT inv.investor_uuid) AS investor_count
    FROM {{ source('bronze', 'dl_funding_table') }} f
    JOIN {{ source('bronze', 'dl_investment_table') }} i ON CAST(f.funding_uuid AS STRING) = CAST(i.funding_uuid AS STRING)
    JOIN {{ source('bronze', 'dl_investor_table') }} inv ON i.investor_uuid = inv.investor_uuid
    GROUP BY f.company_uuid
),

-- Company sector from Inc42 sector thesis (proper classification)
company_sector_thesis AS (
    SELECT
        st.company_uuid,
        STRING_AGG(DISTINCT th.sector, ', ') AS inc42_sectors,
        STRING_AGG(DISTINCT th.sub_sector, ', ') AS inc42_sub_sectors
    FROM {{ source('bronze', 'dl_sector_tagging') }} st
    JOIN {{ source('bronze', 'dl_inc42_sector_thesis') }} th ON st.tag_id = th.tag_id
    GROUP BY st.company_uuid
),

-- Inc42 company tags (Unicorn, Soonicorn, FAST42, etc.)
company_tags AS (
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

-- ═══════════════════════════════════════════════
-- PEOPLE ENRICHMENT (via LinkedIn URL match)
-- ═══════════════════════════════════════════════
people_match AS (
    SELECT linkedin_slug, person_uuid, dl_person_name, dl_person_description
    FROM (
        SELECT
            LOWER(REGEXP_REPLACE(REGEXP_REPLACE(linkedin, r'https?://(www\.)?linkedin\.com/in/', ''), r'[/?].*$', '')) AS linkedin_slug,
            person_uuid,
            name AS dl_person_name,
            long_description AS dl_person_description,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(REGEXP_REPLACE(REGEXP_REPLACE(linkedin, r'https?://(www\.)?linkedin\.com/in/', ''), r'[/?].*$', ''))
                ORDER BY last_modified_date DESC
            ) AS rn
        FROM {{ source('bronze', 'dl_people_table') }}
        WHERE linkedin IS NOT NULL AND linkedin != ''
          AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(linkedin, r'https?://(www\.)?linkedin\.com/in/', ''), r'[/?].*$', '')) > 2
    )
    WHERE rn = 1
),

-- Current job (primary_organisation = 1 means current company)
people_experience AS (
    SELECT people_uuid, job_title, job_seniority, job_function,
           name AS dl_current_company, company_uuid AS experience_company_uuid
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY people_uuid
            ORDER BY COALESCE(SAFE_CAST(primary_organisation AS INT64), 0) DESC, start_date DESC
        ) AS rn
        FROM {{ source('bronze', 'dl_people_description_table') }}
    )
    WHERE rn = 1
),

-- Latest education
people_education AS (
    SELECT people_uuid, education_details AS institution, degree_details AS degree,
           specialisation, degree_category
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY people_uuid ORDER BY start_date DESC NULLS LAST) AS rn
        FROM {{ source('bronze', 'dl_people_education_table') }}
    )
    WHERE rn = 1
),

-- All education (for premier college flag)
people_all_education AS (
    SELECT
        people_uuid,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'indian institute of technology|\\biit\\b') THEN TRUE
            ELSE FALSE
        END) AS is_iit_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'indian institute of management|\\biim\\b') THEN TRUE
            ELSE FALSE
        END) AS is_iim_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'indian school of business|\\bisb\\b') THEN TRUE
            ELSE FALSE
        END) AS is_isb_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'birla institute of technology|\\bbits\\b') THEN TRUE
            ELSE FALSE
        END) AS is_bits_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'narsee monjee|\\bnmims\\b') THEN TRUE
            ELSE FALSE
        END) AS is_nmims_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'harvard|stanford|mit\\b|wharton|oxford|cambridge|insead|london business school|kellogg|yale|columbia university') THEN TRUE
            ELSE FALSE
        END) AS is_global_top_alumni,
        MAX(CASE
            WHEN REGEXP_CONTAINS(LOWER(education_details), r'indian institute of technology|\\biit\\b|indian institute of management|\\biim\\b|indian school of business|\\bisb\\b|birla institute of technology|\\bbits\\b|narsee monjee|\\bnmims\\b|harvard|stanford|mit\\b|wharton|oxford|cambridge|insead|chartered accountant') THEN TRUE
            ELSE FALSE
        END) AS is_premier_institute_alumni
    FROM {{ source('bronze', 'dl_people_education_table') }}
    WHERE education_details IS NOT NULL
    GROUP BY people_uuid
),

-- Is this person an investor? (person_uuid matches P-{uuid} in investor_table)
person_as_investor AS (
    SELECT
        REGEXP_REPLACE(inv.investor_uuid, r'^P-', '') AS person_uuid_clean,
        inv.name AS investor_name,
        COUNT(DISTINCT i.funding_uuid) AS investments_made,
        STRING_AGG(DISTINCT f.name, ', ') AS portfolio_companies
    FROM {{ source('bronze', 'dl_investor_table') }} inv
    JOIN {{ source('bronze', 'dl_investment_table') }} i ON inv.investor_uuid = i.investor_uuid
    JOIN {{ source('bronze', 'dl_funding_table') }} f ON CAST(i.funding_uuid AS STRING) = CAST(f.funding_uuid AS STRING)
    WHERE inv.investor_uuid LIKE 'P-%'
    GROUP BY inv.investor_uuid, inv.name
),

-- ═══════════════════════════════════════════════
-- FACT TABLE AGGREGATIONS
-- ═══════════════════════════════════════════════
orders AS (
    SELECT
        contact_key,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN is_refunded = 1 THEN net_revenue ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN is_completed = 1 THEN net_revenue ELSE 0 END)
            - SUM(CASE WHEN is_refunded = 1 THEN net_revenue ELSE 0 END) AS net_ltv,
        SUM(is_refunded) AS total_refunded_orders,
        SUM(is_cancelled) AS total_cancelled_orders,
        MAX(order_date_key) AS last_order_date_key,
        COUNTIF(product_type = 'membership') AS total_membership_orders,
        COUNTIF(product_type = 'addon') AS total_addon_orders
    FROM {{ ref('fact_orders') }}
    GROUP BY contact_key
),

events AS (
    SELECT
        contact_key,
        COUNT(*) AS total_events_registered,
        SUM(CASE WHEN registration_status = 'registered' THEN 1 ELSE 0 END) AS total_events_active,
        SUM(cancelled_flag) AS total_events_cancelled
    FROM {{ ref('fact_event_attendance') }}
    GROUP BY contact_key
),

forms AS (
    SELECT
        contact_key,
        COUNT(*) AS total_form_submissions,
        COUNT(DISTINCT form_name) AS unique_forms_submitted
    FROM {{ ref('fact_form_submissions') }}
    GROUP BY contact_key
),

marketing AS (
    SELECT
        contact_key,
        SUM(opened) AS total_emails_opened,
        SUM(clicked) AS total_emails_clicked,
        SUM(unsubscribed) AS total_unsubscribes,
        SUM(bounced) AS total_bounced,
        COUNT(*) AS total_touchpoints
    FROM {{ ref('fact_marketing_touchpoints') }}
    GROUP BY contact_key
),

-- ═══════════════════════════════════════════════
-- PROPERTY INTERACTIONS
-- ═══════════════════════════════════════════════
properties AS (
    SELECT DISTINCT contact_key, event_name AS property_name, product_type AS property_type, TRUE AS is_paid
    FROM {{ ref('fact_event_attendance') }}
    WHERE registration_status = 'registered'

    UNION ALL

    SELECT DISTINCT contact_key, form_name AS property_name, 'program' AS property_type, FALSE AS is_paid
    FROM {{ ref('fact_form_submissions') }}
),

property_agg AS (
    SELECT
        contact_key,
        COUNT(DISTINCT property_name) AS total_properties_interacted,
        STRING_AGG(DISTINCT property_name, ', ') AS properties_interacted_names,
        COUNT(DISTINCT CASE WHEN is_paid THEN property_name END) AS total_paid_properties,
        STRING_AGG(DISTINCT CASE WHEN is_paid THEN property_name END, ', ') AS paid_properties_names,
        COUNT(DISTINCT property_type) AS property_types_touched,
        STRING_AGG(DISTINCT property_type, ', ') AS property_types_names
    FROM properties
    GROUP BY contact_key
)

-- ═══════════════════════════════════════════════
-- FINAL SELECT
-- ═══════════════════════════════════════════════
SELECT
    -- ═══ IDENTITY ═══
    c.contact_key,
    c.unified_contact_id,
    c.email,
    c.first_name,
    c.last_name,
    CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, '')) AS full_name,
    c.phone,

    -- ═══ PROFESSIONAL ═══
    c.company_name,
    COALESCE(c.designation, pe.job_title) AS designation,
    COALESCE(c.seniority, CAST(pe.job_seniority AS STRING)) AS seniority,
    pe.job_function,
    c.linkedin_url,
    c.city,
    c.state,
    c.country,
    c.user_type,

    -- ═══ DATALABS PERSON (via LinkedIn match) ═══
    pm.person_uuid AS dl_person_id,
    pm.dl_person_description,
    pe.dl_current_company,                         -- current company from Datalabs people experience

    -- Education
    edu.institution AS dl_education_institution,
    edu.degree AS dl_education_degree,
    edu.specialisation AS dl_education_specialisation,

    -- Premier institute alumni flags
    COALESCE(pae.is_iit_alumni, FALSE) AS is_iit_alumni,
    COALESCE(pae.is_iim_alumni, FALSE) AS is_iim_alumni,
    COALESCE(pae.is_isb_alumni, FALSE) AS is_isb_alumni,
    COALESCE(pae.is_bits_alumni, FALSE) AS is_bits_alumni,
    COALESCE(pae.is_nmims_alumni, FALSE) AS is_nmims_alumni,
    COALESCE(pae.is_global_top_alumni, FALSE) AS is_global_top_alumni,
    COALESCE(pae.is_premier_institute_alumni, FALSE) AS is_premier_institute_alumni,

    -- Is this person an investor?
    CASE WHEN pai.investments_made IS NOT NULL THEN TRUE ELSE FALSE END AS is_investor,
    pai.investments_made AS person_investments_made,
    pai.portfolio_companies AS person_portfolio_companies,

    -- ═══ COMPANY ENRICHMENT (from Datalabs) ═══
    cm.company_uuid AS dl_company_id,

    -- Sector (from Inc42 sector thesis — proper classification)
    cst.inc42_sectors AS company_sector,
    cst.inc42_sub_sectors AS company_sub_sectors,
    cm.dl_business_model AS company_business_model,
    cm.dl_founded_year AS company_founded_year,
    cm.dl_company_website AS company_website,
    cm.dl_company_linkedin AS company_linkedin,

    -- Funding
    COALESCE(cf.total_funding_usd, 0) AS company_total_funding_usd,
    cf.last_funding_date AS company_last_funding_date,
    cf.last_funding_stage AS company_last_funding_stage,
    COALESCE(cf.total_funding_rounds, 0) AS company_total_funding_rounds,

    -- Financials
    cpl.revenue_from_operations AS company_revenue,
    cpl.profit_loss_for_the_period AS company_profit_loss,
    cpl.is_profitable AS company_is_profitable,

    -- Employees & Web
    cemp.employee_count AS company_employee_count,
    cweb.monthly_visits AS company_monthly_visits,

    -- Company investors (who funded this company)
    ci.investors AS company_investors,
    COALESCE(ci.investor_count, 0) AS company_investor_count,

    -- Inc42 tags with categories
    ct.inc42_tags AS company_inc42_tags,
    COALESCE(ct.is_unicorn, FALSE) AS company_is_unicorn,
    COALESCE(ct.is_soonicorn, FALSE) AS company_is_soonicorn,
    COALESCE(ct.is_minicorn, FALSE) AS company_is_minicorn,
    COALESCE(ct.is_fast42_winner, FALSE) AS company_is_fast42_winner,
    COALESCE(ct.is_30_startups_to_watch, FALSE) AS company_is_30_startups_to_watch,
    COALESCE(ct.is_startup_watchlist, FALSE) AS company_is_startup_watchlist,
    COALESCE(ct.is_inc42_upnext, FALSE) AS company_is_inc42_upnext,
    COALESCE(ct.is_tracked, FALSE) AS company_is_tracked,

    -- ═══ ORDERS ═══
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    COALESCE(o.total_refunds, 0) AS total_refunds,
    COALESCE(o.net_ltv, 0) AS net_ltv,
    COALESCE(o.total_refunded_orders, 0) AS total_refunded_orders,
    COALESCE(o.total_cancelled_orders, 0) AS total_cancelled_orders,
    COALESCE(o.total_membership_orders, 0) AS total_membership_orders,
    COALESCE(o.total_addon_orders, 0) AS total_addon_orders,

    -- ═══ EVENTS ═══
    COALESCE(ev.total_events_registered, 0) AS total_events_registered,
    COALESCE(ev.total_events_active, 0) AS total_events_active,
    COALESCE(ev.total_events_cancelled, 0) AS total_events_cancelled,

    -- ═══ FORMS ═══
    COALESCE(f.total_form_submissions, 0) AS total_form_submissions,
    COALESCE(f.unique_forms_submitted, 0) AS unique_forms_submitted,

    -- ═══ MARKETING ═══
    COALESCE(m.total_emails_opened, 0) AS total_emails_opened,
    COALESCE(m.total_emails_clicked, 0) AS total_emails_clicked,
    COALESCE(m.total_unsubscribes, 0) AS total_unsubscribes,
    COALESCE(m.total_bounced, 0) AS total_bounced,
    COALESCE(m.total_touchpoints, 0) AS total_touchpoints,

    -- ═══ NEWSLETTERS ═══
    c.daily_newsletter,
    c.weekly_newsletter,
    c.indepth_newsletter,
    c.moneyball_newsletter,
    c.theoutline_newsletter,
    c.markets_newsletter,
    c.is_globally_unsubscribed,
    c.is_suppressed,
    c.whatsapp_optin,
    c.email_reachability,
    (CASE WHEN c.daily_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.weekly_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.indepth_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.moneyball_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.theoutline_newsletter = 'subscribed' THEN 1 ELSE 0 END
     + CASE WHEN c.markets_newsletter = 'subscribed' THEN 1 ELSE 0 END
    ) AS total_newsletters_subscribed,

    -- ═══ ENGAGEMENT SCORE ═══
    ROUND(
        COALESCE(m.total_emails_opened, 0) * 2.0
        + COALESCE(m.total_emails_clicked, 0) * 5.0
        + COALESCE(ev.total_events_registered, 0) * 10.0
        + COALESCE(f.total_form_submissions, 0) * 8.0
        + COALESCE(o.total_orders, 0) * 15.0
    , 1) AS engagement_score,

    -- ═══ PROPERTY INTERACTIONS ═══
    COALESCE(p.total_properties_interacted, 0) AS total_properties_interacted,
    p.properties_interacted_names,
    COALESCE(p.total_paid_properties, 0) AS total_paid_properties,
    p.paid_properties_names,
    COALESCE(p.property_types_touched, 0) AS property_types_touched,
    p.property_types_names,

    -- ═══ SOURCE COVERAGE ═══
    c.source_count,
    c.found_in_systems,
    c.all_emails,
    c.all_phones,

    CURRENT_TIMESTAMP() AS updated_at

FROM contact c

-- Fact aggregations
LEFT JOIN orders o ON c.contact_key = o.contact_key
LEFT JOIN events ev ON c.contact_key = ev.contact_key
LEFT JOIN forms f ON c.contact_key = f.contact_key
LEFT JOIN marketing m ON c.contact_key = m.contact_key
LEFT JOIN property_agg p ON c.contact_key = p.contact_key

-- Company enrichment (via company name)
LEFT JOIN company_match cm ON LOWER(TRIM(c.company_name)) = cm.company_name_lower
LEFT JOIN company_funding cf ON cm.company_uuid = cf.company_uuid
LEFT JOIN company_financials cpl ON cm.company_uuid = cpl.company_uuid
LEFT JOIN company_employees cemp ON cm.company_uuid = cemp.company_uuid
LEFT JOIN company_web cweb ON cm.company_uuid = cweb.company_uuid
LEFT JOIN company_investors ci ON cm.company_uuid = ci.company_uuid
LEFT JOIN company_tags ct ON cm.company_uuid = ct.company_uuid
LEFT JOIN company_sector_thesis cst ON cm.company_uuid = cst.company_uuid

-- People enrichment (via LinkedIn URL slug)
LEFT JOIN people_match pm ON LOWER(REGEXP_REPLACE(REGEXP_REPLACE(c.linkedin_url, r'https?://(www\.)?linkedin\.com/in/', ''), r'[/?].*$', '')) = pm.linkedin_slug
    AND c.linkedin_url IS NOT NULL AND c.linkedin_url != ''
LEFT JOIN people_experience pe ON pm.person_uuid = pe.people_uuid
LEFT JOIN people_education edu ON pm.person_uuid = edu.people_uuid
LEFT JOIN people_all_education pae ON pm.person_uuid = pae.people_uuid
LEFT JOIN person_as_investor pai ON pm.person_uuid = pai.person_uuid_clean
