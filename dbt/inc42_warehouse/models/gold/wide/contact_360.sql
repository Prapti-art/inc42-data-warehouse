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
    -- Non-event orders only. Event tickets are sourced from silver.events
    -- via the events_summary CTE below, so we don't double-count.
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
        COUNTIF(product_type = 'addon') AS total_addon_orders,
        SUM(CASE WHEN product_type = 'membership' AND is_completed = 1 THEN net_revenue ELSE 0 END) AS total_membership_revenue,
        MAX(CASE WHEN is_completed = 1 THEN order_date_key END) AS last_completed_order_date_key
    FROM {{ ref('fact_orders') }}
    GROUP BY contact_key
),

-- All event interactions sourced from silver.events (franchise-normalized,
-- deduped by contact x franchise x edition x role). Replaces the old
-- `events` CTE + event-specific fields in `orders`.
events_summary AS (
    SELECT
        contact_key,
        COUNT(*) AS total_event_interactions,
        COUNTIF(is_paid) AS total_paid_event_tickets,
        COUNTIF(NOT is_paid) AS total_free_event_registrations,
        COUNT(DISTINCT event_franchise) AS event_franchise_count,
        COUNT(DISTINCT CONCAT(event_franchise, '|', IFNULL(event_edition,''))) AS distinct_franchise_editions,
        STRING_AGG(DISTINCT event_franchise, ', ') AS event_franchises,
        STRING_AGG(DISTINCT CASE WHEN is_paid THEN event_franchise END, ', ') AS paid_event_franchises,
        STRING_AGG(DISTINCT CASE WHEN NOT is_paid THEN event_franchise END, ', ') AS free_event_franchises,
        STRING_AGG(DISTINCT
            CASE WHEN event_edition IS NOT NULL
                 THEN CONCAT(event_franchise, ' ', event_edition)
                 ELSE event_franchise
            END, ', ') AS event_franchise_editions,
        -- Loyalty / recency (numeric editions only — regional editions like "Hyderabad" excluded)
        MIN(SAFE_CAST(event_edition AS INT64)) AS first_event_year,
        MAX(SAFE_CAST(event_edition AS INT64)) AS last_event_year,
        COUNT(DISTINCT CASE WHEN is_paid THEN SAFE_CAST(event_edition AS INT64) END) AS years_as_paid_attendee,
        SUM(CASE WHEN is_paid AND attendance_status = 'paid' THEN net_revenue ELSE 0 END) AS total_event_revenue,
        SUM(refund_amount) AS total_event_refunds,
        COUNTIF(event_format = 'program') AS total_programs_joined,
        STRING_AGG(DISTINCT CASE WHEN event_format = 'program' THEN event_franchise END, ', ') AS programs_joined,
        MAX(interaction_date) AS last_event_interaction_date
    FROM {{ ref('events') }}
    GROUP BY contact_key
),

-- Collapse silver.events to one row per (contact, franchise, year) BEFORE the
-- paid/free split, so "paid wins" holds at the name-list level. Without this,
-- a person who registered (free, role='registrant') AND paid (role='attendee')
-- for the same summit-year would have the event appear in BOTH lists.
event_year_grain AS (
    SELECT
        contact_key,
        event_franchise,
        event_edition,
        LOGICAL_OR(is_paid) AS any_paid
    FROM {{ ref('events') }}
    WHERE event_franchise IS NOT NULL
    GROUP BY contact_key, event_franchise, event_edition
),

event_names_clean AS (
    SELECT
        contact_key,
        STRING_AGG(DISTINCT CASE WHEN any_paid
            THEN CONCAT(event_franchise, ' ', IFNULL(event_edition, '')) END, ', ') AS paid_event_names,
        STRING_AGG(DISTINCT CASE WHEN NOT any_paid
            THEN CONCAT(event_franchise, ' ', IFNULL(event_edition, '')) END, ', ') AS free_event_names,
        COUNTIF(any_paid) AS total_paid_events,
        COUNTIF(NOT any_paid) AS total_free_events,
        COUNT(*) AS total_events_engaged
    FROM event_year_grain
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
-- MOENGAGE ENGAGEMENT (push/email/whatsapp reachability + LTV/session signals)
-- One row per canonical email. Where multiple rows share an email, keep the
-- most recently active record (last_seen DESC) so engagement reflects the
-- latest device/profile.
-- ═══════════════════════════════════════════════
moengage_engagement AS (
    SELECT
        email,
        first_seen AS moengage_first_seen,
        last_seen AS moengage_last_seen,
        SAFE_CAST(ltv AS FLOAT64) AS moengage_ltv,
        SAFE_CAST(no_of_sessions AS INT64) AS moengage_sessions,
        SAFE_CAST(no_of_conversions AS INT64) AS moengage_conversions,
        user_creation_source AS moengage_user_source,
        install_status AS moengage_install_status,
        reachability_push AS moengage_reachability_push,
        reachability_push_android AS moengage_reachability_push_android,
        reachability_push_ios AS moengage_reachability_push_ios,
        reachability_push_web AS moengage_reachability_push_web,
        email_opt_in_status AS moengage_email_optin,
        hard_bounce AS moengage_hard_bounce,
        spam AS moengage_spam_flag,
        unsubscribe AS moengage_unsubscribe_flag,
        sms_subscription_status AS moengage_sms_status,
        web_push_subscription_status AS moengage_web_push_status,
        whatsapp_subscription_status AS moengage_whatsapp_status,
        last_known_city AS moengage_last_city,
        last_known_state AS moengage_last_state,
        last_known_country AS moengage_last_country,
        last_known_pincode AS moengage_last_pincode
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(COALESCE(
                    NULLIF(email_standard, ''),
                    NULLIF(work_email, ''),
                    NULLIF(personal_email, ''),
                    NULLIF(plus_email, ''),
                    NULLIF(secondary_email, '')
                )))
                ORDER BY last_seen DESC NULLS LAST
            ) AS rn,
            LOWER(TRIM(COALESCE(
                NULLIF(email_standard, ''),
                NULLIF(work_email, ''),
                NULLIF(personal_email, ''),
                NULLIF(plus_email, ''),
                NULLIF(secondary_email, '')
            ))) AS email
        FROM {{ source('bronze', 'moengage_export') }}
        WHERE COALESCE(
            NULLIF(email_standard, ''),
            NULLIF(work_email, ''),
            NULLIF(personal_email, ''),
            NULLIF(plus_email, ''),
            NULLIF(secondary_email, '')
        ) IS NOT NULL
    )
    WHERE rn = 1
),

-- ═══════════════════════════════════════════════
-- PROPERTY INTERACTIONS (properly categorized)
-- ═══════════════════════════════════════════════
properties AS (
    -- Memberships (paid via WooCommerce)
    SELECT DISTINCT contact_key,
        product_name AS property_name,
        'membership' AS property_type,
        TRUE AS is_paid
    FROM {{ ref('fact_orders') }}
    WHERE is_successful = 1
      AND product_type = 'membership'

    UNION ALL

    -- Events + programs from silver.events (franchise-normalized, deduped).
    -- property_name is the franchise (D2C Summit, GenAI Summit, FAST42, ...),
    -- not the raw product/form name.
    SELECT DISTINCT contact_key,
        event_franchise AS property_name,
        CASE WHEN event_format = 'program' THEN 'program' ELSE 'event' END AS property_type,
        is_paid
    FROM {{ ref('events') }}
    WHERE event_franchise IS NOT NULL

    UNION ALL

    -- Content (Newsletters, Report Downloads, Giveaways)
    SELECT DISTINCT contact_key,
        form_name AS property_name,
        'content' AS property_type,
        FALSE AS is_paid
    FROM {{ ref('fact_form_submissions') }}
    WHERE form_category IN ('Newsletter', 'Report Download', 'Giveaway')
),

property_agg AS (
    SELECT
        contact_key,
        COUNT(DISTINCT property_name) AS total_properties_interacted,
        STRING_AGG(DISTINCT property_name, ', ') AS properties_interacted_names,

        -- By type
        COUNT(DISTINCT CASE WHEN property_type = 'membership' THEN property_name END) AS membership_properties,
        STRING_AGG(DISTINCT CASE WHEN property_type = 'membership' THEN property_name END, ', ') AS membership_names,

        COUNT(DISTINCT CASE WHEN property_type = 'event' THEN property_name END) AS event_properties,
        STRING_AGG(DISTINCT CASE WHEN property_type = 'event' THEN property_name END, ', ') AS event_names,

        COUNT(DISTINCT CASE WHEN property_type = 'program' THEN property_name END) AS program_properties,
        STRING_AGG(DISTINCT CASE WHEN property_type = 'program' THEN property_name END, ', ') AS program_names,

        COUNT(DISTINCT CASE WHEN property_type = 'content' THEN property_name END) AS content_properties,

        -- Paid vs free
        COUNT(DISTINCT CASE WHEN is_paid THEN property_name END) AS total_paid_properties,
        STRING_AGG(DISTINCT CASE WHEN is_paid THEN property_name END, ', ') AS paid_properties_names,

        COUNT(DISTINCT property_type) AS property_types_touched,
        STRING_AGG(DISTINCT property_type, ', ') AS property_types_names
    FROM properties
    GROUP BY contact_key
),

-- ═══════════════════════════════════════════════
-- INTEREST TAGS — signals merged from 4 sources:
--   (a) Event franchise touched (silver.events)
--   (b) Reports / Giveaways downloaded (fact_form_submissions)
--   (c) Newsletter subscriptions (contact.X_newsletter)
--   (d) Company sector (Datalabs sector_thesis)
-- Each interest is TRUE if ANY source signals it.
-- ═══════════════════════════════════════════════
event_interest_signals AS (
    SELECT
        e.contact_key,
        LOGICAL_OR(e.event_franchise IN ('D2C Summit', 'D2CX Converge', 'FAST42')) AS event_d2c,
        LOGICAL_OR(e.event_franchise IN ('GenAI Summit', 'AI Workshop')) AS event_ai,
        LOGICAL_OR(e.event_franchise = 'Fintech Summit') AS event_fintech,
        LOGICAL_OR(e.event_franchise IN ('FAST42', 'Startup Programs', 'BigShift', 'Inc42 BrandLabs')) AS event_startup_program
    FROM {{ ref('events') }} e
    GROUP BY e.contact_key
),

-- Parse BOTH form_subcategory AND form_name for sector keywords.
-- form_subcategory taxonomy is incomplete (no Healthtech/Edtech/Mobility/etc.
-- as named subcategories), but form_name has the sector terms in plain text.
report_interest_signals AS (
    SELECT
        contact_key,
        LOGICAL_OR(form_subcategory IN ('D2C Report','Ecommerce Report','Consumer Internet Report')
            OR REGEXP_CONTAINS(LOWER(form_name), r'd2c|consumer internet|ecommerce|mattress|online.*shopper')) AS report_d2c,
        LOGICAL_OR(form_subcategory = 'Fintech Report'
            OR REGEXP_CONTAINS(LOWER(form_name), r'fintech|lending tech|insurance ?tech|payment|banking')) AS report_fintech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'\bai\b|gen.?ai|artificial intelligence|machine learning')) AS report_ai,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'\bsaas\b|enterprise tech')) AS report_saas_enterprise,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'health.?tech|healthcare')) AS report_healthtech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'ed.?tech|education tech')) AS report_edtech,
        LOGICAL_OR(form_subcategory = 'EV Giveaway'
            OR REGEXP_CONTAINS(LOWER(form_name), r'cleantech|electric vehicle|\bev\b|renewable')) AS report_cleantech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'agritech|agri.?tech|agriculture')) AS report_agritech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'logistics|mobility')) AS report_logistics_mobility,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'travel.?tech|tourism')) AS report_traveltech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'real.?estate|proptech')) AS report_realestatetech,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'web3|blockchain|crypto|nft')) AS report_web3,
        LOGICAL_OR(REGEXP_CONTAINS(LOWER(form_name), r'gaming|media|entertainment')) AS report_media_gaming,
        LOGICAL_OR(form_subcategory IN (
            'IPO Report','Unicorn Report','Funding Report','Ebook - Angel Investors',
            'Guide - VC Funding','Guide - VC Funds','Top 100 Startups Report',
            'Startup Ecosystem Report','Guide - Accelerators','Guide - Govt Schemes'
        ) OR REGEXP_CONTAINS(LOWER(form_name), r'funding|\bipo\b|unicorn|investor|\bvc\b|angel')) AS report_investor_startup
    FROM {{ ref('fact_form_submissions') }}
    GROUP BY contact_key
),

contact_sectors AS (
    SELECT
        c.contact_key,
        LOWER(COALESCE(cst.inc42_sub_sectors, cst.inc42_sectors, '')) AS sectors_lower
    FROM contact c
    LEFT JOIN company_match cm ON LOWER(TRIM(c.company_name)) = cm.company_name_lower
    LEFT JOIN company_sector_thesis cst ON cm.company_uuid = cst.company_uuid
),

interest_tags AS (
    SELECT
        cs.contact_key,
        COALESCE(es.event_d2c, FALSE) OR COALESCE(rs.report_d2c, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'ecommerce|foodtech|consumer services|consumer internet|d2c') AS interest_d2c,
        COALESCE(es.event_ai, FALSE) OR COALESCE(rs.report_ai, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'\bai\b|machine learning|gen ai|deeptech') AS interest_ai,
        COALESCE(es.event_fintech, FALSE) OR COALESCE(rs.report_fintech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'fintech') AS interest_fintech,
        COALESCE(rs.report_saas_enterprise, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'enterprise tech|saas') AS interest_enterprise_tech,
        COALESCE(rs.report_healthtech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'health tech|healthtech') AS interest_health_tech,
        COALESCE(rs.report_edtech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'edtech|ed tech') AS interest_edtech,
        COALESCE(rs.report_cleantech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'clean tech|cleantech|ev|electric vehicle') AS interest_clean_tech,
        COALESCE(rs.report_agritech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'agritech|agri tech') AS interest_agritech,
        COALESCE(rs.report_logistics_mobility, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'logistics|mobility') AS interest_logistics,
        COALESCE(rs.report_traveltech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'travel tech|traveltech') AS interest_travel_tech,
        COALESCE(rs.report_realestatetech, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'real estate tech|proptech') AS interest_real_estate_tech,
        COALESCE(rs.report_web3, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'web3|blockchain|crypto') AS interest_web3,
        COALESCE(rs.report_media_gaming, FALSE)
            OR REGEXP_CONTAINS(cs.sectors_lower, r'media & entertainment|gaming') AS interest_media_entertainment,
        -- Startup Ecosystem (merged: FAST42/BigShift/BrandLabs/Startup Programs events +
        -- IPO/Unicorn/Funding/Angel/VC/Govt Schemes/Accelerators reports).
        COALESCE(es.event_startup_program, FALSE)
            OR COALESCE(rs.report_investor_startup, FALSE) AS interest_startup_ecosystem
    FROM contact_sectors cs
    LEFT JOIN event_interest_signals es USING(contact_key)
    LEFT JOIN report_interest_signals rs USING(contact_key)
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
    -- Drop designation values that are just the company name extracted from
    -- the email domain (e.g. designation="Acciojob" for yash@acciojob.com).
    -- Fall back to Datalabs job_title in that case.
    CASE
        WHEN LENGTH(COALESCE(c.designation, '')) > 1
            AND LOWER(TRIM(c.designation)) = LOWER(REGEXP_EXTRACT(c.email, r'@([^.]+)'))
        THEN pe.job_title
        ELSE COALESCE(c.designation, pe.job_title)
    END AS designation,
    COALESCE(c.seniority, {{ normalize_seniority('CAST(pe.job_seniority AS STRING)') }}) AS seniority,
    {{ normalize_job_function('pe.job_function') }} AS job_function,
    c.linkedin_url,
    c.city,
    c.state,
    c.country,
    c.user_type,

    -- ═══ HUBSPOT (sales pipeline) ═══
    c.hubspot_contact_id,
    c.hubspot_lifecycle_stage,
    c.hubspot_lead_status,
    c.hubspot_lead_source,
    c.hubspot_score,
    c.hubspot_owner_id,
    c.hubspot_associated_company_ids,
    c.hubspot_created_at,
    c.hubspot_modified_at,

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
    COALESCE(o.total_membership_revenue, 0) AS total_membership_revenue,

    -- ═══ EVENT TICKETS + REGISTRATIONS (sourced from silver.events) ═══
    -- Franchise-normalized + deduped (one row per contact × franchise × edition × role).
    COALESCE(es.total_paid_event_tickets, 0) AS total_event_orders,
    COALESCE(es.total_event_revenue, 0) AS total_event_revenue,
    COALESCE(es.total_event_refunds, 0) AS total_event_refunds,
    SAFE.PARSE_DATE('%Y%m%d', CAST(o.last_completed_order_date_key AS STRING)) AS last_purchase_date,

    COALESCE(es.total_free_event_registrations, 0) AS total_events_registered,
    COALESCE(es.total_paid_event_tickets, 0) AS total_events_attended,
    COALESCE(es.total_programs_joined, 0) AS total_programs_joined,
    COALESCE(es.event_franchise_count, 0) AS event_franchise_count,
    COALESCE(es.distinct_franchise_editions, 0) AS event_franchise_editions_count,
    es.event_franchises,
    -- Year-bearing event lists (paid wins over free per franchise+year)
    enc.paid_event_names,
    enc.free_event_names,
    es.event_franchise_editions AS all_event_names,
    COALESCE(enc.total_paid_events, 0) AS total_paid_events,
    COALESCE(enc.total_free_events, 0) AS total_free_events,
    COALESCE(enc.total_events_engaged, 0) AS total_events_engaged,
    -- Franchise-only rollups (kept for franchise-level filtering)
    es.paid_event_franchises,
    es.free_event_franchises,
    -- Loyalty / recency
    es.first_event_year,
    es.last_event_year,
    COALESCE(es.years_as_paid_attendee, 0) AS years_as_paid_attendee,
    es.programs_joined,
    es.last_event_interaction_date,

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

    -- ═══ ENGAGEMENT SCORES ═══
    -- Three scores per contact, designed for reverse-ETL segmentation:
    --   paid_engagement_score    -> spending behaviour (memberships, paid events,
    --                                addon orders, lifetime revenue)
    --   nonpaid_engagement_score -> content/reach behaviour (newsletters, free
    --                                event regs, reports, email opens/clicks,
    --                                push reachability)
    --   combined_engagement_score = paid * 2 + nonpaid  (paid weighted higher)
    --   engagement_tier          -> dormant / passive / engaged / hot
    ROUND(
        COALESCE(o.total_membership_orders, 0) * 50.0
        + COALESCE(es.total_paid_event_tickets, 0) * 30.0
        + COALESCE(o.total_addon_orders, 0) * 20.0
        + COALESCE(o.total_revenue, 0) * 0.001
    , 1) AS paid_engagement_score,

    ROUND(
        (CASE WHEN c.daily_newsletter = 'subscribed' THEN 1 ELSE 0 END
         + CASE WHEN c.weekly_newsletter = 'subscribed' THEN 1 ELSE 0 END
         + CASE WHEN c.indepth_newsletter = 'subscribed' THEN 1 ELSE 0 END
         + CASE WHEN c.moneyball_newsletter = 'subscribed' THEN 1 ELSE 0 END
         + CASE WHEN c.theoutline_newsletter = 'subscribed' THEN 1 ELSE 0 END
         + CASE WHEN c.markets_newsletter = 'subscribed' THEN 1 ELSE 0 END
        ) * 10.0
        + COALESCE(es.total_free_event_registrations, 0) * 8.0
        + COALESCE(f.total_form_submissions, 0) * 3.0
        + COALESCE(m.total_emails_opened, 0) * 0.5
        + COALESCE(m.total_emails_clicked, 0) * 2.0
        + CASE WHEN me.moengage_reachability_push IN ('300','100') THEN 5.0 ELSE 0 END
    , 1) AS nonpaid_engagement_score,

    ROUND(
        (COALESCE(o.total_membership_orders, 0) * 50.0
         + COALESCE(es.total_paid_event_tickets, 0) * 30.0
         + COALESCE(o.total_addon_orders, 0) * 20.0
         + COALESCE(o.total_revenue, 0) * 0.001) * 2.0
        + (CASE WHEN c.daily_newsletter = 'subscribed' THEN 1 ELSE 0 END
           + CASE WHEN c.weekly_newsletter = 'subscribed' THEN 1 ELSE 0 END
           + CASE WHEN c.indepth_newsletter = 'subscribed' THEN 1 ELSE 0 END
           + CASE WHEN c.moneyball_newsletter = 'subscribed' THEN 1 ELSE 0 END
           + CASE WHEN c.theoutline_newsletter = 'subscribed' THEN 1 ELSE 0 END
           + CASE WHEN c.markets_newsletter = 'subscribed' THEN 1 ELSE 0 END
          ) * 10.0
        + COALESCE(es.total_free_event_registrations, 0) * 8.0
        + COALESCE(f.total_form_submissions, 0) * 3.0
        + COALESCE(m.total_emails_opened, 0) * 0.5
        + COALESCE(m.total_emails_clicked, 0) * 2.0
        + CASE WHEN me.moengage_reachability_push IN ('300','100') THEN 5.0 ELSE 0 END
    , 1) AS combined_engagement_score,

    CASE
        WHEN (COALESCE(o.total_membership_orders, 0) * 50.0
              + COALESCE(es.total_paid_event_tickets, 0) * 30.0
              + COALESCE(o.total_addon_orders, 0) * 20.0
              + COALESCE(o.total_revenue, 0) * 0.001) >= 50
          OR (COALESCE(es.total_free_event_registrations, 0) * 8.0
              + COALESCE(f.total_form_submissions, 0) * 3.0
              + COALESCE(m.total_emails_opened, 0) * 0.5
              + COALESCE(m.total_emails_clicked, 0) * 2.0) >= 100
        THEN 'hot'
        WHEN (COALESCE(o.total_orders, 0) > 0
              OR COALESCE(es.total_paid_event_tickets, 0) > 0
              OR (CASE WHEN c.daily_newsletter='subscribed' THEN 1 ELSE 0 END
                  + CASE WHEN c.weekly_newsletter='subscribed' THEN 1 ELSE 0 END
                  + CASE WHEN c.indepth_newsletter='subscribed' THEN 1 ELSE 0 END
                  + CASE WHEN c.moneyball_newsletter='subscribed' THEN 1 ELSE 0 END
                  + CASE WHEN c.theoutline_newsletter='subscribed' THEN 1 ELSE 0 END
                  + CASE WHEN c.markets_newsletter='subscribed' THEN 1 ELSE 0 END) >= 2
              OR COALESCE(f.total_form_submissions, 0) >= 3
              OR COALESCE(es.total_free_event_registrations, 0) >= 1)
        THEN 'engaged'
        WHEN (CASE WHEN c.daily_newsletter='subscribed' THEN 1 ELSE 0 END
              + CASE WHEN c.weekly_newsletter='subscribed' THEN 1 ELSE 0 END
              + CASE WHEN c.indepth_newsletter='subscribed' THEN 1 ELSE 0 END
              + CASE WHEN c.moneyball_newsletter='subscribed' THEN 1 ELSE 0 END
              + CASE WHEN c.theoutline_newsletter='subscribed' THEN 1 ELSE 0 END
              + CASE WHEN c.markets_newsletter='subscribed' THEN 1 ELSE 0 END) >= 1
          OR COALESCE(f.total_form_submissions, 0) >= 1
          OR COALESCE(m.total_emails_opened, 0) >= 1
          OR me.moengage_reachability_push IN ('300','100')
        THEN 'passive'
        ELSE 'dormant'
    END AS engagement_tier,

    -- ═══ PROPERTY INTERACTIONS ═══
    COALESCE(p.total_properties_interacted, 0) AS total_properties_interacted,
    p.properties_interacted_names,
    COALESCE(p.total_paid_properties, 0) AS total_paid_properties,
    p.paid_properties_names,
    COALESCE(p.property_types_touched, 0) AS property_types_touched,
    p.property_types_names,

    -- By type
    COALESCE(p.membership_properties, 0) AS membership_properties,
    p.membership_names,
    COALESCE(p.event_properties, 0) AS event_properties,
    p.event_names,
    COALESCE(p.program_properties, 0) AS program_properties,
    p.program_names,
    COALESCE(p.content_properties, 0) AS content_properties,

    -- ═══ INTEREST TAGS (Inc42 sector nomenclature) ═══
    -- Signals: silver.events franchise + fact_form_submissions (Reports/Giveaways
    -- by subcategory + form_name keywords) + Datalabs sector + newsletter subs.
    -- Newsletters: theoutline -> d2c; moneyball+markets -> startup_ecosystem.
    COALESCE(it.interest_d2c, FALSE) OR c.theoutline_newsletter = 'subscribed' AS interest_d2c,
    COALESCE(it.interest_ai, FALSE) AS interest_ai,
    COALESCE(it.interest_fintech, FALSE) AS interest_fintech,
    COALESCE(it.interest_enterprise_tech, FALSE) AS interest_enterprise_tech,
    COALESCE(it.interest_health_tech, FALSE) AS interest_health_tech,
    COALESCE(it.interest_edtech, FALSE) AS interest_edtech,
    COALESCE(it.interest_clean_tech, FALSE) AS interest_clean_tech,
    COALESCE(it.interest_agritech, FALSE) AS interest_agritech,
    COALESCE(it.interest_logistics, FALSE) AS interest_logistics,
    COALESCE(it.interest_travel_tech, FALSE) AS interest_travel_tech,
    COALESCE(it.interest_real_estate_tech, FALSE) AS interest_real_estate_tech,
    COALESCE(it.interest_web3, FALSE) AS interest_web3,
    COALESCE(it.interest_media_entertainment, FALSE) AS interest_media_entertainment,
    COALESCE(it.interest_startup_ecosystem, FALSE)
        OR c.moneyball_newsletter = 'subscribed'
        OR c.markets_newsletter = 'subscribed' AS interest_startup_ecosystem,

    -- ═══ MOENGAGE ENGAGEMENT (reachability + sessions/LTV) ═══
    me.moengage_first_seen,
    me.moengage_last_seen,
    me.moengage_ltv,
    me.moengage_sessions,
    me.moengage_conversions,
    me.moengage_user_source,
    me.moengage_install_status,
    me.moengage_reachability_push,
    me.moengage_reachability_push_android,
    me.moengage_reachability_push_ios,
    me.moengage_reachability_push_web,
    me.moengage_email_optin,
    me.moengage_hard_bounce,
    me.moengage_spam_flag,
    me.moengage_unsubscribe_flag,
    me.moengage_sms_status,
    me.moengage_web_push_status,
    me.moengage_whatsapp_status,
    me.moengage_last_city,
    me.moengage_last_state,
    me.moengage_last_country,
    me.moengage_last_pincode,

    -- ═══ SOURCE COVERAGE ═══
    c.source_count,
    c.found_in_systems,
    c.all_emails,
    c.all_phones,

    CURRENT_TIMESTAMP() AS updated_at

FROM contact c

-- Fact aggregations
LEFT JOIN orders o ON c.contact_key = o.contact_key
LEFT JOIN events_summary es ON c.contact_key = es.contact_key
LEFT JOIN event_names_clean enc ON c.contact_key = enc.contact_key
LEFT JOIN forms f ON c.contact_key = f.contact_key
LEFT JOIN marketing m ON c.contact_key = m.contact_key
LEFT JOIN property_agg p ON c.contact_key = p.contact_key
LEFT JOIN moengage_engagement me ON LOWER(c.email) = me.email

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
LEFT JOIN interest_tags it ON c.contact_key = it.contact_key
