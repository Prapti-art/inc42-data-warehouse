-- Silver contacts: one row per person with best-available fields from ALL sources
-- Maximizes data completeness by extracting every useful field

WITH unified AS (
    SELECT * FROM {{ source('silver', 'unified_contacts') }}
),

-- ── Inc42 DB: users + usermeta (pivoted — ALL useful meta_keys) ──
inc42 AS (
    SELECT
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
),

-- ── Customer.io: people + attributes (pivoted — ALL useful attributes) ──
customerio AS (
    SELECT
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
gravity_form88 AS (
    SELECT email, company_name, seniority, industry, phone
    FROM (
        SELECT
            LOWER(TRIM(MAX(CASE WHEN m.meta_key = '3' THEN m.meta_value END))) AS email,
            MAX(CASE WHEN m.meta_key = '4' THEN m.meta_value END) AS company_name,
            MAX(CASE WHEN m.meta_key = '5' THEN m.meta_value END) AS seniority,
            MAX(CASE WHEN m.meta_key = '6' THEN m.meta_value END) AS phone,
            MAX(CASE WHEN m.meta_key = '9' THEN m.meta_value END) AS industry,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(MAX(CASE WHEN m.meta_key = '3' THEN m.meta_value END)))
                ORDER BY e.date_created DESC
            ) AS rn
        FROM {{ source('bronze', 'gravity_forms_entries') }} e
        JOIN {{ source('bronze', 'gravity_forms_entry_meta') }} m ON e.id = m.entry_id
        WHERE e.form_id = 88 AND e.status = 'active'
        GROUP BY e.id, e.date_created
    )
    WHERE rn = 1 AND email IS NOT NULL AND email LIKE '%@%'
)

SELECT
    u.unified_contact_id,
    u.primary_email AS email,

    -- ═══ NAME ═══
    COALESCE(
        NULLIF(i.first_name, ''), NULLIF(c.first_name, ''),
        NULLIF(w.first_name, ''), NULLIF(t.first_name, ''), u.first_name
    ) AS first_name,
    COALESCE(
        NULLIF(i.last_name, ''), NULLIF(c.last_name, ''),
        NULLIF(w.last_name, ''), NULLIF(t.last_name, ''), u.last_name
    ) AS last_name,

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

    -- ═══ COMPANY ═══
    COALESCE(
        NULLIF(i.company_name, ''), NULLIF(c.company_name, ''),
        NULLIF(w.company_name, ''), NULLIF(t.company_name, ''),
        NULLIF(gf.company_name, ''), u.primary_company
    ) AS company_name,
    NULLIF(c.company_website, '') AS company_website,

    -- ═══ DESIGNATION ═══
    COALESCE(
        NULLIF(i.designation, ''), NULLIF(i.user_designation, ''),
        NULLIF(i.working_designation, ''), NULLIF(c.designation, ''),
        NULLIF(t.designation, '')
    ) AS designation,

    -- ═══ SENIORITY ═══ (Inc42 108K > CIO 19K > GF88 87K > WooCommerce > Tally)
    COALESCE(
        NULLIF(i.seniority, ''), NULLIF(c.seniority, ''),
        NULLIF(gf.seniority, ''), NULLIF(w.seniority, ''), NULLIF(t.seniority, '')
    ) AS seniority,

    -- ═══ INDUSTRY ═══ (Inc42 44K > CIO 8K > GF88 87K > Tally)
    COALESCE(
        NULLIF(i.industry, ''), NULLIF(c.industry, ''),
        NULLIF(gf.industry, ''), NULLIF(t.sector, '')
    ) AS industry,

    -- ═══ LINKEDIN ═══ (Inc42 24K > CIO 1.7K > Tally)
    COALESCE(
        NULLIF(i.linkedin_url, ''), NULLIF(c.linkedin_url, ''), NULLIF(t.linkedin_url, '')
    ) AS linkedin_url,

    -- ═══ LOCATION ═══
    COALESCE(NULLIF(i.city, ''), NULLIF(c.city, ''), NULLIF(w.city, ''), NULLIF(t.city, '')) AS city,
    COALESCE(NULLIF(i.state, ''), NULLIF(c.region, ''), NULLIF(w.state, '')) AS state,
    COALESCE(NULLIF(i.country, ''), NULLIF(c.country, ''), NULLIF(w.country, '')) AS country,
    COALESCE(NULLIF(i.postcode, ''), NULLIF(c.postal_code, ''), NULLIF(w.postcode, '')) AS postcode,
    COALESCE(NULLIF(i.address, ''), NULLIF(w.address, '')) AS address,
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
    i.company_function,
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
