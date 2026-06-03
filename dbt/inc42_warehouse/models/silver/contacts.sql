-- Silver contacts: one row per person with best-available fields from ALL sources
-- Maximizes data completeness by extracting every useful field

WITH unified AS (
    SELECT * FROM {{ source('silver', 'unified_contacts') }}
),

-- ── Inc42 DB: users + usermeta (pivoted — ALL useful meta_keys) ──
-- Wrapped in QUALIFY to keep one row per email (latest user_registered wins)
-- so downstream LEFT JOIN cannot fan out when a single email has multiple user IDs.
inc42 AS (
    SELECT * EXCEPT(rn) FROM (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(u.user_email))
            ORDER BY u.user_registered DESC, u.ID DESC
        ) AS rn,
        LOWER(TRIM(u.user_email)) AS email,
        MAX(CASE WHEN m.meta_key = 'first_name' THEN m.meta_value END) AS first_name,
        MAX(CASE WHEN m.meta_key = 'last_name' THEN m.meta_value END) AS last_name,
        MAX(CASE WHEN m.meta_key = 'billing_phone' THEN m.meta_value END) AS phone,
        MAX(CASE WHEN m.meta_key = 'billing_company' THEN m.meta_value END) AS company_name,
        MAX(CASE WHEN m.meta_key = 'billing_designation' THEN m.meta_value END) AS designation,
        MAX(CASE WHEN m.meta_key = '_user_designation' THEN m.meta_value END) AS user_designation,
        MAX(CASE WHEN m.meta_key = '_user_working_designation' THEN m.meta_value END) AS working_designation,
        MAX(CASE WHEN m.meta_key = 'billing_seniority' THEN m.meta_value END) AS seniority,
        MAX(CASE WHEN m.meta_key = 'billing_city' THEN m.meta_value END) AS city,
        MAX(CASE WHEN m.meta_key = 'billing_state' THEN m.meta_value END) AS state,
        MAX(CASE WHEN m.meta_key = 'billing_country' THEN m.meta_value END) AS country,
        MAX(CASE WHEN m.meta_key = 'billing_address_1' THEN m.meta_value END) AS address,
        MAX(CASE WHEN m.meta_key = 'billing_postcode' THEN m.meta_value END) AS postcode,
        MAX(CASE WHEN m.meta_key = 'billing_gst' THEN m.meta_value END) AS gst_number,
        MAX(CASE WHEN m.meta_key = 'company_type' THEN m.meta_value END) AS company_type,
        MAX(CASE WHEN m.meta_key = 'company_function' THEN m.meta_value END) AS company_function,
        MAX(CASE WHEN m.meta_key IN ('industry', 'billing_industry') THEN m.meta_value END) AS industry,
        MAX(CASE WHEN m.meta_key = 'investor_type' THEN m.meta_value END) AS investor_type,
        MAX(CASE WHEN m.meta_key IN ('linked_in', 'linkedin_profile', 'linkedin') THEN m.meta_value END) AS linkedin_url,
        MAX(CASE WHEN m.meta_key = 'signup_source' THEN m.meta_value END) AS signup_source,
        MAX(CASE WHEN m.meta_key = 'allow_newsletter' THEN m.meta_value END) AS allow_newsletter,
        MAX(CASE WHEN m.meta_key = 'variable_icp' THEN m.meta_value END) AS variable_icp,
        MAX(CASE WHEN m.meta_key = 'fixed_icp' THEN m.meta_value END) AS fixed_icp,
        MAX(CASE WHEN m.meta_key = 'is_trial_user' THEN m.meta_value END) AS is_trial_user,
        MAX(CASE WHEN m.meta_key = 'trial_start_date' THEN m.meta_value END) AS trial_start_date,
        MAX(CASE WHEN m.meta_key = 'trial_end_date' THEN m.meta_value END) AS trial_end_date,
        MAX(CASE WHEN m.meta_key = 'paying_customer' THEN m.meta_value END) AS paying_customer,
        MAX(CASE WHEN m.meta_key = '_money_spent' THEN m.meta_value END) AS money_spent,
        MAX(CASE WHEN m.meta_key = '_order_count' THEN m.meta_value END) AS order_count,
        MAX(CASE WHEN m.meta_key = 'billing_email' THEN m.meta_value END) AS billing_email,
        MAX(CASE WHEN m.meta_key = 'twitter' THEN m.meta_value END) AS twitter,
        MAX(CASE WHEN m.meta_key = 'facebook' THEN m.meta_value END) AS facebook,
        MAX(CASE WHEN m.meta_key = 'description' THEN m.meta_value END) AS bio,
        u.user_registered
    FROM {{ source('bronze', 'inc42_users') }} u
    LEFT JOIN {{ source('bronze', 'inc42_usermeta') }} m ON u.ID = m.user_id
    WHERE u.user_email IS NOT NULL AND u.user_email != ''
    GROUP BY u.ID, u.user_email, u.user_registered
    ) WHERE rn = 1
),

-- ── Customer.io: people + attributes (pivoted — ALL useful attributes) ──
-- Wrapped in ROW_NUMBER to keep one row per email so downstream JOIN can't fan out.
customerio AS (
    SELECT * EXCEPT(rn) FROM (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(p.email_addr))
            ORDER BY p.internal_customer_id DESC
        ) AS rn,
        LOWER(TRIM(p.email_addr)) AS email,
        MAX(COALESCE(p.suppressed, FALSE)) AS is_suppressed,
        MAX(CASE WHEN a.attribute_name = 'First_Name' THEN a.attribute_value END) AS first_name,
        MAX(CASE WHEN a.attribute_name = 'Last_Name' THEN a.attribute_value END) AS last_name,
        MAX(CASE WHEN a.attribute_name = 'Full_Name' THEN a.attribute_value END) AS full_name,
        MAX(CASE WHEN a.attribute_name = 'Phone Number' THEN a.attribute_value END) AS phone,
        MAX(CASE WHEN a.attribute_name = 'Mobile_Number' THEN a.attribute_value END) AS mobile_number,
        MAX(CASE WHEN a.attribute_name IN ('Company_Name', 'Company Name') THEN a.attribute_value END) AS company_name,
        MAX(CASE WHEN a.attribute_name = 'Designation' THEN a.attribute_value END) AS designation,
        MAX(CASE WHEN a.attribute_name = 'Seniority' THEN a.attribute_value END) AS seniority,
        MAX(CASE WHEN a.attribute_name = 'Industry' THEN a.attribute_value END) AS industry,
        MAX(CASE WHEN a.attribute_name = 'Company_Type' THEN a.attribute_value END) AS company_type,
        MAX(CASE WHEN a.attribute_name = 'Company_Website_Link' THEN a.attribute_value END) AS company_website,
        MAX(CASE WHEN a.attribute_name = 'LinkedIn_Profile_URL' THEN a.attribute_value END) AS linkedin_url,
        MAX(CASE WHEN a.attribute_name = 'cio_city' THEN a.attribute_value END) AS city,
        MAX(CASE WHEN a.attribute_name = 'cio_region' THEN a.attribute_value END) AS region,
        MAX(CASE WHEN a.attribute_name = 'cio_country' THEN a.attribute_value END) AS country,
        MAX(CASE WHEN a.attribute_name = 'cio_timezone' THEN a.attribute_value END) AS timezone,
        MAX(CASE WHEN a.attribute_name = 'cio_postal_code' THEN a.attribute_value END) AS postal_code,
        MAX(CASE WHEN a.attribute_name = 'User_Type' THEN a.attribute_value END) AS user_type,
        MAX(CASE WHEN a.attribute_name = 'Register_Source' THEN a.attribute_value END) AS register_source,
        MAX(CASE WHEN a.attribute_name = 'Registration_Status' THEN a.attribute_value END) AS registration_status,
        MAX(CASE WHEN a.attribute_name IN ('Work_Email', 'Work Email') THEN a.attribute_value END) AS work_email,
        MAX(CASE WHEN a.attribute_name IN ('Personal Email') THEN a.attribute_value END) AS personal_email,
        MAX(CASE WHEN a.attribute_name = 'Plus_Email' THEN a.attribute_value END) AS plus_email,
        MAX(CASE WHEN a.attribute_name = 'Plus_Membership_Type' THEN a.attribute_value END) AS plus_membership_type,
        MAX(CASE WHEN a.attribute_name = 'Plus_Membership_Start_Date' THEN a.attribute_value END) AS plus_start_date,
        MAX(CASE WHEN a.attribute_name = 'Plus_Membership_Expiry_Date' THEN a.attribute_value END) AS plus_expiry_date,
        MAX(CASE WHEN a.attribute_name = 'Plus_Next_Payment_Date' THEN a.attribute_value END) AS plus_next_payment_date,
        MAX(CASE WHEN a.attribute_name = 'Plus_Subscription_ID' THEN a.attribute_value END) AS plus_subscription_id,
        MAX(CASE WHEN a.attribute_name = 'Whatsapp_Optin' THEN a.attribute_value END) AS whatsapp_optin,
        MAX(CASE WHEN a.attribute_name = 'Registered_Date' THEN a.attribute_value END) AS registered_date,
        -- Newsletter subscriptions
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_1') END) AS daily_newsletter,
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_2') END) AS weekly_newsletter,
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_3') END) AS indepth_newsletter,
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_4') END) AS moneyball_newsletter,
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_5') END) AS theoutline_newsletter,
        MAX(CASE WHEN a.attribute_name = 'cio_subscription_preferences'
            THEN JSON_VALUE(a.attribute_value, '$.topics.topic_6') END) AS markets_newsletter,
        MAX(CASE WHEN a.attribute_name = 'unsubscribed' THEN a.attribute_value END) AS unsubscribed_all
    FROM {{ source('bronze', 'cio_people') }} p
    LEFT JOIN {{ source('bronze', 'cio_attributes') }} a ON p.internal_customer_id = a.internal_customer_id
    WHERE p.email_addr IS NOT NULL AND p.email_addr != ''
    GROUP BY p.internal_customer_id, p.email_addr
    ) WHERE rn = 1
),

-- ── WooCommerce: order_meta (pivoted, deduped to latest order per email) ──
woo AS (
    SELECT email, first_name, last_name, phone, company_name, city, state, country,
           seniority, gst, address, postcode
    FROM (
        SELECT
            LOWER(TRIM(MAX(CASE WHEN m.meta_key = '_billing_email' THEN m.meta_value END))) AS email,
            MAX(CASE WHEN m.meta_key = '_billing_first_name' THEN m.meta_value END) AS first_name,
            MAX(CASE WHEN m.meta_key = '_billing_last_name' THEN m.meta_value END) AS last_name,
            MAX(CASE WHEN m.meta_key = '_billing_phone' THEN m.meta_value END) AS phone,
            MAX(CASE WHEN m.meta_key = '_billing_company' THEN m.meta_value END) AS company_name,
            MAX(CASE WHEN m.meta_key = '_billing_city' THEN m.meta_value END) AS city,
            MAX(CASE WHEN m.meta_key = '_billing_state' THEN m.meta_value END) AS state,
            MAX(CASE WHEN m.meta_key = '_billing_country' THEN m.meta_value END) AS country,
            MAX(CASE WHEN m.meta_key IN ('_billing_seniority', 'billing_seniority') THEN m.meta_value END) AS seniority,
            MAX(CASE WHEN m.meta_key = '_billing_gst' THEN m.meta_value END) AS gst,
            MAX(CASE WHEN m.meta_key = '_billing_address_1' THEN m.meta_value END) AS address,
            MAX(CASE WHEN m.meta_key = '_billing_postcode' THEN m.meta_value END) AS postcode,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(MAX(CASE WHEN m.meta_key = '_billing_email' THEN m.meta_value END)))
                ORDER BY o.post_date DESC
            ) AS rn
        FROM {{ source('bronze', 'woocommerce_orders') }} o
        JOIN {{ source('bronze', 'woocommerce_order_meta') }} m ON o.ID = m.post_id
        GROUP BY o.ID, o.post_date
    )
    WHERE rn = 1 AND email IS NOT NULL
),

-- ── Tally: dedup to one row per email (latest submission wins) ──
tally AS (
    SELECT email, first_name, last_name, phone, company_name,
           designation, linkedin_url, city, sector, seniority
    FROM (
        SELECT
            LOWER(TRIM(COALESCE(email, work_email))) AS email,
            first_name,
            last_name,
            COALESCE(phone, whatsapp_number) AS phone,
            company_name,
            designation,
            linkedin_url,
            city,
            sector,
            seniority,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(COALESCE(email, work_email)))
                ORDER BY submitted_at DESC
            ) AS rn
        FROM {{ source('bronze', 'tally_forms') }}
        WHERE COALESCE(email, work_email) IS NOT NULL
    )
    WHERE rn = 1
),

-- ── Gravity Forms Form 88: seniority + industry + company (87K entries) ──
-- ── Gravity Forms — universal per-form field-map driven extract ──
-- Replaces the form-88-only naive `meta_key IN ('3','4','5','6','9')` extract.
-- Joins each entry_meta row to silver.gravity_form_field_map on (form_id, field_id=meta_key)
-- so we look up each value by its CANONICAL field, not its meta_key index. Eliminates the
-- meta_key=10 collision where forms 1/44/54/73/75 had "Lead Source" but form 88 had company.
-- Lead-source values are never mapped to company_name now.
gravity_form88 AS (
    SELECT email, company_name, designation, seniority, industry, phone,
           first_name, last_name, full_name, linkedin_url, city, state, country
    FROM (
        SELECT
            LOWER(TRIM(MAX(CASE WHEN fm.canonical_field = 'email' THEN m.meta_value END))) AS email,
            MAX(CASE WHEN fm.canonical_field = 'company_name'   THEN m.meta_value END) AS company_name,
            MAX(CASE WHEN fm.canonical_field = 'designation'    THEN m.meta_value END) AS designation,
            MAX(CASE WHEN fm.canonical_field = 'seniority'      THEN m.meta_value END) AS seniority,
            MAX(CASE WHEN fm.canonical_field = 'industry'       THEN m.meta_value END) AS industry,
            MAX(CASE WHEN fm.canonical_field = 'phone'          THEN m.meta_value END) AS phone,
            MAX(CASE WHEN fm.canonical_field = 'first_name'     THEN m.meta_value END) AS first_name,
            MAX(CASE WHEN fm.canonical_field = 'last_name'      THEN m.meta_value END) AS last_name,
            MAX(CASE WHEN fm.canonical_field = 'full_name'      THEN m.meta_value END) AS full_name,
            MAX(CASE WHEN fm.canonical_field = 'linkedin_url'   THEN m.meta_value END) AS linkedin_url,
            MAX(CASE WHEN fm.canonical_field = 'city'           THEN m.meta_value END) AS city,
            MAX(CASE WHEN fm.canonical_field = 'state'          THEN m.meta_value END) AS state,
            MAX(CASE WHEN fm.canonical_field = 'country'        THEN m.meta_value END) AS country,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(MAX(CASE WHEN fm.canonical_field = 'email' THEN m.meta_value END)))
                ORDER BY e.date_created DESC
            ) AS rn
        FROM {{ source('bronze', 'gravity_forms_entries') }} e
        JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} m ON e.id = m.entry_id
        LEFT JOIN {{ ref('gravity_form_field_map') }} fm
          ON fm.form_id = e.form_id AND fm.field_id = m.meta_key
        WHERE e.status = 'active'
        GROUP BY e.id, e.date_created
    )
    WHERE rn = 1 AND email IS NOT NULL AND email LIKE '%@%'
),

-- ── HubSpot CRM: contacts from sales pipeline (deduped in silver.hubspot_contacts_latest) ──
hubspot AS (
    SELECT
        email,
        hubspot_contact_id,
        first_name,
        last_name,
        phone_e164 AS phone,
        company_name,
        job_title AS designation,
        linkedin_url,
        city,
        state,
        country,
        lifecycle_stage,
        lead_status,
        lead_source,
        hubspot_score,
        hubspot_owner_id,
        associated_company_ids,
        hubspot_created_at,
        hubspot_modified_at
    FROM {{ ref('hubspot_contacts_latest') }}
    WHERE email IS NOT NULL AND email LIKE '%@%'
),

-- ── Moengage: LinkedIn extraction only (one row per email, most recent profile) ──
-- Moengage CSV holds linkedin in 3 columns: linkedin_profile_url / linkedinurl / linkedin.
-- This contact-side join is what surfaces those for event-buyer contacts who came
-- in via WooCommerce/Gravity Forms (neither captures linkedin in source).
moengage_li AS (
    SELECT email, linkedin_url FROM (
        SELECT
            LOWER(TRIM(COALESCE(
                NULLIF(email_standard, ''), NULLIF(work_email, ''),
                NULLIF(personal_email, ''), NULLIF(plus_email, ''),
                NULLIF(secondary_email, '')
            ))) AS email,
            COALESCE(
                NULLIF(linkedin_profile_url, ''),
                NULLIF(linkedinurl, ''),
                NULLIF(linkedin, '')
            ) AS linkedin_url,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(COALESCE(
                    NULLIF(email_standard, ''), NULLIF(work_email, ''),
                    NULLIF(personal_email, ''), NULLIF(plus_email, ''),
                    NULLIF(secondary_email, '')
                )))
                ORDER BY last_seen DESC NULLS LAST
            ) AS rn
        FROM {{ source('bronze', 'moengage_export') }}
        WHERE COALESCE(linkedin_profile_url, linkedinurl, linkedin) IS NOT NULL
          AND COALESCE(email_standard, work_email, personal_email, plus_email, secondary_email) IS NOT NULL
    )
    WHERE rn = 1 AND linkedin_url IS NOT NULL AND email IS NOT NULL
),

-- ── Gravity Forms: LinkedIn extraction across ALL forms via per-form field detection ──
-- For each form, find the meta_key whose values look like LinkedIn URLs (>=30% match
-- linkedin.com/in/ pattern) and the meta_key whose values look like emails (>=50%).
-- Join the two per entry → one (email, linkedin) per submission. Dedupe to one per
-- email, keeping the most recent. Mirrors the email-field detection used in PySpark.
gravity_linkedin AS (
    WITH email_field_per_form AS (
        SELECT form_id, meta_key FROM (
            SELECT form_id, meta_key,
                COUNTIF(REGEXP_CONTAINS(meta_value, r'@[a-zA-Z0-9.-]+\.')) AS email_cnt,
                COUNT(*) AS total,
                ROW_NUMBER() OVER (
                    PARTITION BY form_id
                    ORDER BY COUNTIF(REGEXP_CONTAINS(meta_value, r'@[a-zA-Z0-9.-]+\.')) DESC
                ) AS rn
            FROM {{ source('bronze', 'gravity_forms_entry_meta') }}
            GROUP BY form_id, meta_key
            HAVING total > 50 AND SAFE_DIVIDE(email_cnt, total) > 0.5
        )
        WHERE rn = 1
    ),
    linkedin_field_per_form AS (
        -- Tightened to >= 70% match (was 30%) so borderline "team bio"-style
        -- free-text fields are no longer flagged as the LinkedIn field.
        SELECT form_id, meta_key FROM (
            SELECT form_id, meta_key,
                COUNTIF(REGEXP_CONTAINS(LOWER(meta_value), r'linkedin\.com/in/')) AS li_cnt,
                COUNT(*) AS total,
                ROW_NUMBER() OVER (
                    PARTITION BY form_id
                    ORDER BY COUNTIF(REGEXP_CONTAINS(LOWER(meta_value), r'linkedin\.com/in/')) DESC
                ) AS rn
            FROM {{ source('bronze', 'gravity_forms_entry_meta') }}
            GROUP BY form_id, meta_key
            HAVING total > 20 AND SAFE_DIVIDE(li_cnt, total) >= 0.7
        )
        WHERE rn = 1
    ),
    extracted AS (
        SELECT
            LOWER(TRIM(em.meta_value)) AS email,
            li.meta_value AS linkedin_url,
            e.date_created,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(em.meta_value)) ORDER BY e.date_created DESC
            ) AS rn
        FROM {{ source('bronze', 'gravity_forms_entries') }} e
        JOIN email_field_per_form ef ON e.form_id = ef.form_id
        JOIN linkedin_field_per_form lf ON e.form_id = lf.form_id
        JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} em
          ON em.entry_id = e.id AND em.form_id = e.form_id AND em.meta_key = ef.meta_key
        JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} li
          ON li.entry_id = e.id AND li.form_id = e.form_id AND li.meta_key = lf.meta_key
        WHERE em.meta_value LIKE '%@%'
          AND LOWER(li.meta_value) LIKE '%linkedin.com/in/%'
    )
    -- Final guard: even after the tightened field detection, each row's value
    -- must actually contain 'linkedin.com' to be pushed forward. Belt-and-braces
    -- against any junk that slips through the per-form detection.
    SELECT email, linkedin_url FROM extracted
    WHERE rn = 1 AND LOWER(linkedin_url) LIKE '%linkedin.com%'
)

SELECT
    u.unified_contact_id,
    u.primary_email AS email,

    -- ═══ NAME ═══
    {{ title_case_clean('COALESCE(NULLIF(i.first_name, ""), NULLIF(c.first_name, ""), NULLIF(w.first_name, ""), NULLIF(t.first_name, ""), u.first_name)') }} AS first_name,
    {{ title_case_clean('COALESCE(NULLIF(i.last_name, ""), NULLIF(c.last_name, ""), NULLIF(w.last_name, ""), NULLIF(t.last_name, ""), u.last_name)') }} AS last_name,

    -- ═══ PHONE ═══ (PySpark normalized, filter fake numbers)
    CASE
        WHEN u.primary_phone IN (
            '+919999999999', '+910000000000', '+911111111111', '+919999988888',
            '+919999999990', '+919999999991', '+919999999998', '+919999900000',
            '+919999911111', '+919999990000'
        ) THEN NULL
        WHEN u.primary_phone IS NOT NULL AND LENGTH(REGEXP_REPLACE(u.primary_phone, r'[^0-9]', '')) < 10
            THEN NULL
        ELSE u.primary_phone
    END AS phone,

    -- ═══ COMPANY ═══ (scrub_company_name_junk nulls form-attribution leakage
    -- like 'source' from meta_key collisions + employment-status entries)
    {{ scrub_company_name_junk('COALESCE(NULLIF(i.company_name, ""), NULLIF(c.company_name, ""), NULLIF(w.company_name, ""), NULLIF(t.company_name, ""), NULLIF(gf.company_name, ""), NULLIF(h.company_name, ""), u.primary_company)') }} AS company_name,
    {{ scrub_sentinel('c.company_website', allow_short=False) }} AS company_website,

    -- ═══ DESIGNATION ═══
    {{ normalize_designation('COALESCE(NULLIF(i.designation, ""), NULLIF(i.user_designation, ""), NULLIF(i.working_designation, ""), NULLIF(c.designation, ""), NULLIF(t.designation, ""), NULLIF(h.designation, ""))') }} AS designation,

    -- ═══ SENIORITY ═══ (normalized to fixed bucket set)
    -- Prefer an explicit seniority field; if none, derive seniority from the
    -- designation/title (e.g. "Store Manager" -> Manager, "Founder" -> Founder).
    COALESCE(
        {{ normalize_seniority('COALESCE(NULLIF(i.seniority, ""), NULLIF(c.seniority, ""), NULLIF(gf.seniority, ""), NULLIF(w.seniority, ""), NULLIF(t.seniority, ""))') }},
        {{ normalize_seniority('COALESCE(NULLIF(i.designation, ""), NULLIF(i.user_designation, ""), NULLIF(i.working_designation, ""), NULLIF(c.designation, ""), NULLIF(t.designation, ""), NULLIF(h.designation, ""))') }}
    ) AS seniority,

    -- ═══ INDUSTRY ═══ (Inc42 44K > CIO 8K > GF88 87K > Tally)
    {{ scrub_sentinel('COALESCE(NULLIF(i.industry, ""), NULLIF(c.industry, ""), NULLIF(gf.industry, ""), NULLIF(t.sector, ""))', allow_short=False) }} AS industry,

    -- ═══ LINKEDIN ═══ (Inc42 24K > CIO 1.7K > Tally > HubSpot > Gravity Forms > Moengage)
    {{ scrub_sentinel('COALESCE(NULLIF(i.linkedin_url, ""), NULLIF(c.linkedin_url, ""), NULLIF(t.linkedin_url, ""), NULLIF(h.linkedin_url, ""), NULLIF(gl.linkedin_url, ""), NULLIF(ml.linkedin_url, ""))', allow_short=False) }} AS linkedin_url,

    -- ═══ LOCATION (normalized) ═══
    {{ normalize_city('COALESCE(NULLIF(i.city, ""), NULLIF(c.city, ""), NULLIF(w.city, ""), NULLIF(t.city, ""), NULLIF(h.city, ""))') }} AS city,
    {{ normalize_state('COALESCE(NULLIF(i.state, ""), NULLIF(c.region, ""), NULLIF(w.state, ""), NULLIF(h.state, ""))') }} AS state,
    {{ normalize_country('COALESCE(NULLIF(i.country, ""), NULLIF(c.country, ""), NULLIF(w.country, ""), NULLIF(h.country, ""))') }} AS country,
    {{ scrub_sentinel('COALESCE(NULLIF(i.postcode, ""), NULLIF(c.postal_code, ""), NULLIF(w.postcode, ""))', allow_short=False) }} AS postcode,
    {{ scrub_sentinel('COALESCE(NULLIF(i.address, ""), NULLIF(w.address, ""))', allow_short=False) }} AS address,
    c.timezone,

    -- ═══ USER CLASSIFICATION ═══
    CASE
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') IN ('Early Stage Startups') THEN 'Early Stage Startup'
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') IN ('Growth Stage Startups', 'Growth Stage Startup') THEN 'Growth Stage Startup'
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') IN ('Late Stage Startups') THEN 'Late Stage Startup'
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') IN ('Listed Startups') THEN 'Listed Startup'
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') IN ('Indian Corporates') THEN 'Indian Corporate'
        WHEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ') = 'Investors' THEN 'Investor'
        WHEN COALESCE(i.company_type, c.user_type) IS NOT NULL AND TRIM(COALESCE(i.company_type, c.user_type)) != ''
            THEN REPLACE(COALESCE(i.company_type, c.user_type), '_', ' ')
        ELSE NULL
    END AS user_type,
    {{ normalize_job_function('i.company_function') }} AS company_function,
    i.investor_type,

    -- ═══ ICP SCORING ═══
    i.variable_icp,
    i.fixed_icp,

    -- ═══ SIGNUP & REGISTRATION ═══
    COALESCE(NULLIF(i.signup_source, ''), NULLIF(c.register_source, '')) AS signup_source,
    c.registration_status AS cio_registration_status,
    i.user_registered AS inc42_registered_at,
    c.registered_date AS cio_registered_at,

    -- ═══ PLUS MEMBERSHIP (from Customer.io) ═══
    c.plus_membership_type,
    c.plus_start_date,
    c.plus_expiry_date,
    c.plus_next_payment_date,
    c.plus_subscription_id,

    -- ═══ TRIAL INFO (from Inc42 DB) ═══
    CASE WHEN i.is_trial_user = '1' THEN TRUE ELSE FALSE END AS is_trial_user,
    i.trial_start_date,
    i.trial_end_date,

    -- ═══ FINANCIAL (from Inc42 DB) ═══
    i.gst_number,
    SAFE_CAST(i.money_spent AS NUMERIC) AS wp_money_spent,
    SAFE_CAST(i.order_count AS INT64) AS wp_order_count,
    CASE WHEN i.paying_customer = '1' THEN TRUE ELSE FALSE END AS wp_paying_customer,

    -- ═══ ADDITIONAL EMAILS ═══
    c.work_email AS cio_work_email,
    c.personal_email AS cio_personal_email,
    c.plus_email AS cio_plus_email,
    i.billing_email AS inc42_billing_email,

    -- ═══ SOCIAL ═══
    i.twitter,
    i.facebook,
    i.bio,

    -- ═══ NEWSLETTER CONSENT ═══
    i.allow_newsletter AS inc42_newsletter_consent,

    -- ═══ HUBSPOT (sales pipeline) ═══
    h.hubspot_contact_id,
    h.lifecycle_stage AS hubspot_lifecycle_stage,
    h.lead_status AS hubspot_lead_status,
    h.lead_source AS hubspot_lead_source,
    h.hubspot_score,
    h.hubspot_owner_id,
    h.associated_company_ids AS hubspot_associated_company_ids,
    h.hubspot_created_at,
    h.hubspot_modified_at,

    -- ═══ SOURCE COVERAGE ═══
    u.source_count,
    u.found_in_systems,
    u.all_emails,
    u.all_phones,

    -- ═══ NEWSLETTER SUBSCRIPTIONS (from Customer.io) ═══
    CASE WHEN c.daily_newsletter = 'true' THEN 'subscribed'
         WHEN c.daily_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS daily_newsletter,
    CASE WHEN c.weekly_newsletter = 'true' THEN 'subscribed'
         WHEN c.weekly_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS weekly_newsletter,
    CASE WHEN c.indepth_newsletter = 'true' THEN 'subscribed'
         WHEN c.indepth_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS indepth_newsletter,
    CASE WHEN c.moneyball_newsletter = 'true' THEN 'subscribed'
         WHEN c.moneyball_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS moneyball_newsletter,
    CASE WHEN c.theoutline_newsletter = 'true' THEN 'subscribed'
         WHEN c.theoutline_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS theoutline_newsletter,
    CASE WHEN c.markets_newsletter = 'true' THEN 'subscribed'
         WHEN c.markets_newsletter = 'false' THEN 'unsubscribed' ELSE 'unknown' END AS markets_newsletter,
    CASE WHEN c.unsubscribed_all = 'true' THEN TRUE ELSE FALSE END AS is_globally_unsubscribed,
    COALESCE(c.is_suppressed, FALSE) AS is_suppressed,
    COALESCE(c.whatsapp_optin, 'No') AS whatsapp_optin,

    -- ═══ EMAIL REACHABILITY ═══
    CASE
        WHEN c.unsubscribed_all = 'true' THEN 'unsubscribed'
        WHEN c.is_suppressed = TRUE THEN 'suppressed'
        WHEN c.email IS NOT NULL THEN 'reachable'
        ELSE 'not_in_customerio'
    END AS email_reachability,

    CURRENT_TIMESTAMP() AS updated_at

FROM unified u
LEFT JOIN inc42 i ON u.primary_email = i.email
LEFT JOIN customerio c ON u.primary_email = c.email
LEFT JOIN woo w ON u.primary_email = w.email
LEFT JOIN tally t ON u.primary_email = t.email
LEFT JOIN gravity_form88 gf ON u.primary_email = gf.email
LEFT JOIN hubspot h ON u.primary_email = h.email
LEFT JOIN gravity_linkedin gl ON u.primary_email = gl.email
LEFT JOIN moengage_li ml ON u.primary_email = ml.email

-- Exclude test/junk contacts
WHERE u.primary_email IS NOT NULL
  AND u.primary_email NOT LIKE '%test%'
  AND u.primary_email NOT LIKE '%testing%'
  AND u.primary_email NOT LIKE '%example%'
  AND u.primary_email NOT LIKE '%sample%'
  AND u.primary_email NOT LIKE '%mailinator%'
  AND u.primary_email NOT LIKE '../%'
  AND u.primary_email NOT LIKE './%'
  AND COALESCE(u.first_name, '') NOT LIKE '%test%'
  AND REGEXP_CONTAINS(u.primary_email, r'^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$')
