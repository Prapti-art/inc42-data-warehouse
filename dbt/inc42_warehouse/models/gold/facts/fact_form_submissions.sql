-- Gold fact_form_submissions: one row per form submission (Gravity Forms + Tally)
-- Includes normalized form_category, form_subcategory, edition

WITH gf_email_fields AS (
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

gf_form_names AS (
    SELECT CAST(id AS INT64) AS form_id, title AS form_name
    FROM {{ source('bronze', 'gravity_forms_forms') }}
),

gravity_forms AS (
    SELECT
        CAST(e.id AS STRING) AS submission_id,
        MAX(CASE WHEN m.meta_key = ef.email_field THEN LOWER(TRIM(m.meta_value)) END) AS email,
        CAST(e.form_id AS STRING) AS form_id,
        COALESCE(fn.form_name, CAST(e.form_id AS STRING)) AS form_name,
        e.date_created AS submit_date,
        'gravity_forms' AS source_system
    FROM {{ source('bronze', 'gravity_forms_entries') }} e
    JOIN gf_email_fields ef ON e.form_id = ef.form_id
    JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} m
        ON e.id = m.entry_id AND e.form_id = m.form_id
    LEFT JOIN gf_form_names fn ON e.form_id = fn.form_id
    WHERE e.status = 'active'
    GROUP BY e.id, e.form_id, e.date_created, fn.form_name
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
),

-- ═══════════════════════════════════════════════
-- FORM NAME NORMALIZATION
-- ═══════════════════════════════════════════════
normalized AS (
    SELECT
        *,
        LOWER(form_name) AS fn,

        -- ── FORM CATEGORY ──
        CASE
            -- Test
            WHEN LOWER(form_name) LIKE '%test%' THEN 'Test'

            -- Newsletter
            WHEN LOWER(form_name) LIKE '%newsletter%'
              OR LOWER(form_name) LIKE '%newsletter%'
              OR LOWER(form_name) LIKE '%optin%'
              OR LOWER(form_name) LIKE '%amp popup%'
              OR LOWER(form_name) LIKE '%stories that matter%'
              OR LOWER(form_name) LIKE '%weekly subscribers%'
              OR LOWER(form_name) LIKE '%manage subscription%'
              OR LOWER(form_name) LIKE '%confirm email%'
              OR LOWER(form_name) LIKE '%genai subscribed%'
              THEN 'Newsletter'

            -- FAST42
            WHEN LOWER(form_name) LIKE '%fast42%' THEN 'FAST42'

            -- AI Workshop
            WHEN LOWER(form_name) LIKE '%ai workshop%' THEN 'AI Workshop'

            -- GenAI Summit
            WHEN LOWER(form_name) LIKE '%genai summit%' THEN 'GenAI Summit'

            -- D2C Events
            WHEN LOWER(form_name) LIKE '%d2cx converge%' THEN 'D2C Event'
            WHEN LOWER(form_name) LIKE '%d2c day%' THEN 'D2C Event'
            WHEN LOWER(form_name) LIKE '%d2c retreat%' THEN 'D2C Event'
            WHEN LOWER(form_name) LIKE '%d2c%summit%' THEN 'D2C Event'
            WHEN LOWER(form_name) LIKE '%d2c%retail%' THEN 'D2C Event'
            WHEN LOWER(form_name) LIKE '%d2c whatsapp%' THEN 'D2C Event'

            -- Report Download
            WHEN LOWER(form_name) LIKE '%report%'
              OR LOWER(form_name) LIKE '%download%'
              OR LOWER(form_name) LIKE '%state of%'
              OR LOWER(form_name) LIKE '%funding report%'
              OR LOWER(form_name) LIKE '%unicorn%'
              OR LOWER(form_name) LIKE '%decoding%'
              OR LOWER(form_name) LIKE '%indo-japan%'
              OR LOWER(form_name) LIKE '%online mattress%'
              OR form_name = 'Inc42 Reports - Global Form'
              THEN 'Report Download'

            -- Webinar
            WHEN LOWER(form_name) LIKE '%webinar%'
              OR LOWER(form_name) LIKE '%niti aayog%'
              OR LOWER(form_name) LIKE '%100x vc%'
              OR LOWER(form_name) LIKE '%inc42 ama%'
              OR LOWER(form_name) LIKE '%inc42 show%'
              THEN 'Webinar'

            -- Giveaway
            WHEN LOWER(form_name) LIKE '%giveaway%'
              OR LOWER(form_name) LIKE '%ebook%'
              OR LOWER(form_name) LIKE '%50+%startup%scheme%'
              OR LOWER(form_name) LIKE '%top 30 accelerator%'
              OR LOWER(form_name) LIKE '%top 33 active%'
              OR LOWER(form_name) LIKE '%vc funding guide%'
              THEN 'Giveaway'

            -- Startup Program
            WHEN LOWER(form_name) LIKE '%startup submission%'
              OR LOWER(form_name) LIKE '%startup spotlight%'
              OR LOWER(form_name) LIKE '%startup deals%'
              OR LOWER(form_name) LIKE '%bigshift%'
              THEN 'Startup Program'

            -- Sponsored/Partner
            WHEN LOWER(form_name) LIKE '%microsoft%'
              OR LOWER(form_name) LIKE '%onfido%'
              OR LOWER(form_name) LIKE '%karix%'
              OR LOWER(form_name) LIKE '%messagebird%'
              OR LOWER(form_name) LIKE '%freshworks%'
              OR LOWER(form_name) LIKE '%udemy%'
              OR LOWER(form_name) LIKE '%qlik%'
              THEN 'Sponsored/Partner'

            -- Contact
            WHEN LOWER(form_name) LIKE '%get in touch%'
              OR LOWER(form_name) LIKE '%stay connected%'
              OR LOWER(form_name) LIKE '%contact form%'
              OR LOWER(form_name) LIKE '%love to hear%'
              OR LOWER(form_name) LIKE '%edit your profile%'
              THEN 'Contact'

            -- Datalabs
            WHEN LOWER(form_name) LIKE '%datalabs%' THEN 'Datalabs'

            -- BrandLabs
            WHEN LOWER(form_name) LIKE '%brandlabs%' THEN 'BrandLabs'

            -- Survey
            WHEN LOWER(form_name) LIKE '%survey%' THEN 'Survey'

            ELSE 'Other'
        END AS form_category,

        -- ── FORM SUBCATEGORY ──
        CASE
            -- Newsletter subcategories
            WHEN LOWER(form_name) LIKE '%manage subscription%' THEN 'Manage'
            WHEN LOWER(form_name) LIKE '%confirm email%' THEN 'Manage'
            WHEN LOWER(form_name) LIKE '%weekly subscribers%' THEN 'Weekly Subscribers'
            WHEN LOWER(form_name) LIKE '%newsletter%'
              OR LOWER(form_name) LIKE '%optin%'
              OR LOWER(form_name) LIKE '%amp popup%'
              OR LOWER(form_name) LIKE '%stories that matter%'
              OR LOWER(form_name) LIKE '%genai subscribed%'
              THEN 'Signup'

            -- FAST42
            WHEN LOWER(form_name) LIKE '%fast42%conclave%' THEN 'Conclave'
            WHEN LOWER(form_name) LIKE '%fast42%' THEN 'Application'

            -- AI Workshop
            WHEN LOWER(form_name) LIKE '%ai workshop%onboarding%' THEN 'Onboarding'
            WHEN LOWER(form_name) LIKE '%ai workshop%hr leader%' THEN 'HR Leaders'
            WHEN LOWER(form_name) LIKE '%ai workshop%startup leader%' THEN 'Startup Leaders'
            WHEN LOWER(form_name) LIKE '%ai workshop%team%hr%' THEN 'Teams - HR'
            WHEN LOWER(form_name) LIKE '%ai workshop%team%startup%' THEN 'Teams - Startup'
            WHEN LOWER(form_name) LIKE '%waitlist%' THEN 'Waitlist'

            -- GenAI Summit
            WHEN LOWER(form_name) LIKE '%genai summit%sponsor%' THEN 'Sponsor'
            WHEN LOWER(form_name) LIKE '%genai summit%application%' THEN 'Application'
            WHEN LOWER(form_name) LIKE '%genai summit%' THEN 'Application'

            -- D2C subcategories
            WHEN LOWER(form_name) LIKE '%d2cx converge%' THEN 'D2CX Converge'
            WHEN LOWER(form_name) LIKE '%d2c day%' THEN 'D2C Day'
            WHEN LOWER(form_name) LIKE '%d2c retreat%' THEN 'D2C Retreat'
            WHEN LOWER(form_name) LIKE '%d2c%summit%' OR LOWER(form_name) LIKE '%d2c%retail%' THEN 'D2C Summit'
            WHEN LOWER(form_name) LIKE '%d2c whatsapp%' THEN 'D2C Whatsapp'

            -- Report subcategories
            WHEN LOWER(form_name) LIKE '%funding%report%' OR form_name = 'Inc42 Reports - Global Form' THEN 'Funding Report'
            WHEN LOWER(form_name) LIKE '%fintech%' THEN 'Fintech Report'
            WHEN LOWER(form_name) LIKE '%ecommerce%' THEN 'Ecommerce Report'
            WHEN LOWER(form_name) LIKE '%consumer internet%' THEN 'Consumer Internet Report'
            WHEN LOWER(form_name) LIKE '%startup ecosystem%' THEN 'Startup Ecosystem Report'
            WHEN LOWER(form_name) LIKE '%d2c%report%' OR LOWER(form_name) LIKE '%d2c%market%' THEN 'D2C Report'
            WHEN LOWER(form_name) LIKE '%ipo%' OR LOWER(form_name) LIKE '%decoding boat%' THEN 'IPO Report'
            WHEN LOWER(form_name) LIKE '%unicorn%' THEN 'Unicorn Report'
            WHEN LOWER(form_name) LIKE '%top 100 startup%' OR LOWER(form_name) LIKE '%financials%india%top%' THEN 'Top 100 Startups Report'
            WHEN LOWER(form_name) LIKE '%sponsored%' THEN 'Sponsored Report'
            WHEN LOWER(form_name) LIKE '%report%' OR LOWER(form_name) LIKE '%download%' OR LOWER(form_name) LIKE '%state of%' THEN 'Other Report'

            -- Webinar
            WHEN LOWER(form_name) LIKE '%niti aayog%' THEN 'Niti Aayog'
            WHEN LOWER(form_name) LIKE '%100x vc%' THEN '100X VC'
            WHEN LOWER(form_name) LIKE '%inc42 ama%' THEN 'Inc42 AMA'
            WHEN LOWER(form_name) LIKE '%inc42 show%' THEN 'Inc42 Show'

            -- Giveaway
            WHEN LOWER(form_name) LIKE '%angel investor%ebook%' THEN 'Ebook - Angel Investors'
            WHEN LOWER(form_name) LIKE '%vc funding guide%' THEN 'Guide - VC Funding'
            WHEN LOWER(form_name) LIKE '%startup scheme%' THEN 'Guide - Govt Schemes'
            WHEN LOWER(form_name) LIKE '%accelerator%' THEN 'Guide - Accelerators'
            WHEN LOWER(form_name) LIKE '%active vc fund%' THEN 'Guide - VC Funds'
            WHEN LOWER(form_name) LIKE '%ev giveaway%' THEN 'EV Giveaway'

            -- Startup Program
            WHEN LOWER(form_name) LIKE '%startup submission%' THEN 'Submission'
            WHEN LOWER(form_name) LIKE '%startup spotlight%' THEN 'Spotlight'
            WHEN LOWER(form_name) LIKE '%startup deals%' THEN 'Deals'
            WHEN LOWER(form_name) LIKE '%bigshift%' THEN 'BigShift'

            -- Sponsored/Partner
            WHEN LOWER(form_name) LIKE '%microsoft%' THEN 'Microsoft'
            WHEN LOWER(form_name) LIKE '%onfido%' THEN 'Onfido'
            WHEN LOWER(form_name) LIKE '%karix%' THEN 'Karix'
            WHEN LOWER(form_name) LIKE '%messagebird%' THEN 'MessageBird'
            WHEN LOWER(form_name) LIKE '%freshworks%' THEN 'Freshworks'
            WHEN LOWER(form_name) LIKE '%udemy%' THEN 'Udemy'
            WHEN LOWER(form_name) LIKE '%qlik%' THEN 'Qlik'

            -- Contact
            WHEN LOWER(form_name) LIKE '%get in touch%' THEN 'Get In Touch'
            WHEN LOWER(form_name) LIKE '%stay connected%' THEN 'Stay Connected'
            WHEN LOWER(form_name) LIKE '%contact form%' THEN 'Inc42 Plus'
            WHEN LOWER(form_name) LIKE '%love to hear%' THEN 'Feedback'
            WHEN LOWER(form_name) LIKE '%edit your profile%' THEN 'Edit Profile'

            -- Datalabs
            WHEN LOWER(form_name) LIKE '%datalabs%demo%' THEN 'Demo'

            -- BrandLabs
            WHEN LOWER(form_name) LIKE '%brandlabs%' THEN 'Application'

            -- Survey
            WHEN LOWER(form_name) LIKE '%ai needs%' THEN 'AI Needs'

            ELSE NULL
        END AS form_subcategory,

        -- ── EDITION ──
        CASE
            -- FAST42 editions
            WHEN LOWER(form_name) LIKE '%fast42%2024%' THEN '2024'
            WHEN LOWER(form_name) LIKE '%fast42%d2c edition%2025%' THEN 'D2C Edition 2025'
            WHEN LOWER(form_name) LIKE '%fast42%5th edition%' THEN '5th Edition'
            WHEN LOWER(form_name) LIKE '%fast42%6th edition%' THEN '6th Edition'
            WHEN LOWER(form_name) LIKE '%fast42%conclave%2026%' THEN '2026 Delhi NCR'

            -- D2C editions
            WHEN LOWER(form_name) LIKE '%d2cx converge%all edition%' THEN 'All Edition'
            WHEN LOWER(form_name) LIKE '%d2cx converge%hyderabad%' THEN 'Hyderabad'
            WHEN form_name = 'THE D2C DAY' THEN '1.0'
            WHEN LOWER(form_name) LIKE '%d2c day 2.0%' THEN '2.0'
            WHEN LOWER(form_name) LIKE '%d2c retreat%2025%' THEN '2025'
            WHEN LOWER(form_name) LIKE '%d2c retreat%' THEN '2025'
            WHEN LOWER(form_name) LIKE '%d2c%summit%2025%' OR LOWER(form_name) LIKE '%d2c%retail%summit%' THEN '2025'

            -- AI Workshop editions (by date)
            WHEN LOWER(form_name) LIKE '%6th%7th september%' THEN 'Sep 2024'
            WHEN LOWER(form_name) LIKE '%20th%21st september%' THEN 'Sep 2024'
            WHEN LOWER(form_name) LIKE '%1st%2nd november%' THEN 'Nov 2024'
            WHEN LOWER(form_name) LIKE '%22nd november%' THEN 'Nov 2024'
            WHEN LOWER(form_name) LIKE '%19th december%' THEN 'Dec 2024'
            WHEN LOWER(form_name) LIKE '%13th%14th%' THEN 'Dec 2024'

            -- GenAI Summit editions
            WHEN LOWER(form_name) LIKE '%genai summit%2024%' THEN '2024'
            WHEN LOWER(form_name) LIKE '%genai summit%2025%' THEN '2025'

            -- Report editions (year/quarter)
            WHEN LOWER(form_name) LIKE '%h1 2021%' THEN 'H1 2021'
            WHEN LOWER(form_name) LIKE '%q3 2021%' THEN 'Q3 2021'
            WHEN LOWER(form_name) LIKE '%2021%' AND LOWER(form_name) NOT LIKE '%h1%' AND LOWER(form_name) NOT LIKE '%q3%' THEN '2021'
            WHEN LOWER(form_name) LIKE '%q1 2022%' THEN 'Q1 2022'
            WHEN LOWER(form_name) LIKE '%q2 2022%' THEN 'Q2 2022'
            WHEN LOWER(form_name) LIKE '%q3 2022%' THEN 'Q3 2022'
            WHEN LOWER(form_name) LIKE '%h1 2022%' THEN 'H1 2022'
            WHEN LOWER(form_name) LIKE '%2022%' AND LOWER(form_name) NOT LIKE '%q%2022%' AND LOWER(form_name) NOT LIKE '%h1 2022%' THEN '2022'

            -- Webinar editions
            WHEN LOWER(form_name) LIKE '%covid%' THEN 'Covid-19'
            WHEN LOWER(form_name) LIKE '%15th may%' THEN 'May 2020'

            -- Newsletter specific types
            WHEN LOWER(form_name) LIKE '%lead magnet%' THEN 'Lead Magnet'
            WHEN LOWER(form_name) LIKE '%(market)%' THEN 'Moneyball'
            WHEN LOWER(form_name) LIKE '%(money ball)%' THEN 'Moneyball'
            WHEN LOWER(form_name) LIKE '%(weekly)%' THEN 'Weekly'
            WHEN LOWER(form_name) LIKE '%(indepth)%' THEN 'InDepth'
            WHEN LOWER(form_name) LIKE '%(iit mafia)%' THEN 'TheOutline'
            WHEN LOWER(form_name) LIKE '%lock optin%' THEN 'Lock Optin'

            ELSE NULL
        END AS edition

    FROM all_forms
)

SELECT
    n.submission_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(n.submit_date)) AS INT64) AS submit_date_key,
    n.form_id,
    n.form_name,
    n.form_category,
    n.form_subcategory,
    n.edition,
    n.source_system,
    1 AS submission_count

FROM normalized n
LEFT JOIN {{ ref('dim_contact') }} dc ON n.email = dc.email
WHERE n.form_category != 'Test'
