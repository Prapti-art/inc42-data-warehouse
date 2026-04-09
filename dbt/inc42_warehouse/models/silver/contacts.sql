-- Silver contacts: one row per person with best-available fields from all sources
-- Priority: Inc42 DB (most complete) > Customer.io > WooCommerce > Tally > Gravity Forms

WITH unified AS (
    SELECT * FROM {{ source('silver', 'unified_contacts') }}
),

xref AS (
    SELECT * FROM {{ source('silver', 'contact_source_xref') }}
),

-- ── Inc42 DB: users + usermeta (pivoted) ──
inc42 AS (
    SELECT
        LOWER(TRIM(u.user_email)) AS email,
        MAX(CASE WHEN m.meta_key = 'first_name' THEN m.meta_value END) AS first_name,
        MAX(CASE WHEN m.meta_key = 'last_name' THEN m.meta_value END) AS last_name,
        MAX(CASE WHEN m.meta_key = 'billing_phone' THEN m.meta_value END) AS phone,
        MAX(CASE WHEN m.meta_key = 'billing_company' THEN m.meta_value END) AS company_name,
        MAX(CASE WHEN m.meta_key = 'billing_designation' THEN m.meta_value END) AS designation,
        MAX(CASE WHEN m.meta_key = 'billing_city' THEN m.meta_value END) AS city,
        MAX(CASE WHEN m.meta_key = 'company_type' THEN m.meta_value END) AS company_type,
        MAX(CASE WHEN m.meta_key = 'company_function' THEN m.meta_value END) AS company_function,
        MAX(CASE WHEN m.meta_key = '_user_designation' THEN m.meta_value END) AS user_designation,
        u.user_registered
    FROM {{ source('bronze', 'inc42_users') }} u
    LEFT JOIN {{ source('bronze', 'inc42_usermeta') }} m ON u.ID = m.user_id
    WHERE u.user_email IS NOT NULL AND u.user_email != ''
    GROUP BY u.ID, u.user_email, u.user_registered
),

-- ── Customer.io: people + attributes (pivoted) ──
customerio AS (
    SELECT
        LOWER(TRIM(p.email_addr)) AS email,
        MAX(COALESCE(p.suppressed, FALSE)) AS is_suppressed,
        MAX(CASE WHEN a.attribute_name = 'First_Name' THEN a.attribute_value END) AS first_name,
        MAX(CASE WHEN a.attribute_name = 'Last_Name' THEN a.attribute_value END) AS last_name,
        MAX(CASE WHEN a.attribute_name = 'Phone Number' THEN a.attribute_value END) AS phone,
        MAX(CASE WHEN a.attribute_name IN ('Company_Name', 'Company Name') THEN a.attribute_value END) AS company_name,
        MAX(CASE WHEN a.attribute_name = 'cio_city' THEN a.attribute_value END) AS city,
        MAX(CASE WHEN a.attribute_name = 'LinkedIn_Profile_URL' THEN a.attribute_value END) AS linkedin_url,
        MAX(CASE WHEN a.attribute_name = 'Work_Email' THEN a.attribute_value END) AS work_email,
        MAX(CASE WHEN a.attribute_name = 'Full_Name' THEN a.attribute_value END) AS full_name,
        MAX(CASE WHEN a.attribute_name = 'Whatsapp_Optin' THEN a.attribute_value END) AS whatsapp_optin,
        -- Newsletter subscriptions from cio_subscription_preferences
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
    SELECT email, first_name, last_name, phone, company_name, city, state, country, seniority
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
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(TRIM(MAX(CASE WHEN m.meta_key = '_billing_email' THEN m.meta_value END)))
                ORDER BY o.post_date DESC
            ) AS rn
        FROM {{ source('bronze', 'woocommerce_orders') }} o
        JOIN {{ source('bronze', 'woocommerce_order_meta') }} m ON o.order_id = m.order_id
        GROUP BY o.order_id, o.post_date
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
)

SELECT
    u.unified_contact_id,
    u.primary_email AS email,

    -- Name: Inc42 > CIO > WooCommerce > Tally
    COALESCE(
        NULLIF(i.first_name, ''),
        NULLIF(c.first_name, ''),
        NULLIF(w.first_name, ''),
        NULLIF(t.first_name, ''),
        u.first_name
    ) AS first_name,
    COALESCE(
        NULLIF(i.last_name, ''),
        NULLIF(c.last_name, ''),
        NULLIF(w.last_name, ''),
        NULLIF(t.last_name, ''),
        u.last_name
    ) AS last_name,

    -- Phone: PySpark already normalized
    u.primary_phone AS phone,

    -- Company
    COALESCE(
        NULLIF(i.company_name, ''),
        NULLIF(c.company_name, ''),
        NULLIF(w.company_name, ''),
        NULLIF(t.company_name, ''),
        u.primary_company
    ) AS company_name,

    -- Professional
    COALESCE(NULLIF(i.designation, ''), NULLIF(i.user_designation, ''), NULLIF(t.designation, '')) AS designation,
    COALESCE(NULLIF(w.seniority, ''), NULLIF(t.seniority, '')) AS seniority,
    COALESCE(NULLIF(c.linkedin_url, ''), NULLIF(t.linkedin_url, '')) AS linkedin_url,

    -- Location
    COALESCE(NULLIF(i.city, ''), NULLIF(c.city, ''), NULLIF(w.city, ''), NULLIF(t.city, '')) AS city,
    w.state,
    w.country,

    -- User type from Inc42 DB
    i.company_type AS user_type,

    -- Source coverage
    u.source_count,
    u.found_in_systems,
    u.all_emails,
    u.all_phones,

    -- Newsletter subscriptions (from Customer.io)
    CASE WHEN c.daily_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS daily_newsletter,
    CASE WHEN c.weekly_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS weekly_newsletter,
    CASE WHEN c.indepth_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS indepth_newsletter,
    CASE WHEN c.moneyball_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS moneyball_newsletter,
    CASE WHEN c.theoutline_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS theoutline_newsletter,
    CASE WHEN c.markets_newsletter = 'true' THEN 'subscribed' ELSE 'unsubscribed' END AS markets_newsletter,
    CASE WHEN c.unsubscribed_all = 'true' THEN TRUE ELSE FALSE END AS is_globally_unsubscribed,
    COALESCE(c.is_suppressed, FALSE) AS is_suppressed,
    COALESCE(c.whatsapp_optin, 'No') AS whatsapp_optin,

    -- Email reachability status (based on Customer.io)
    CASE
        WHEN c.unsubscribed_all = 'true' THEN 'unsubscribed'
        WHEN c.is_suppressed = TRUE THEN 'suppressed'
        WHEN c.email IS NOT NULL THEN 'reachable'
        ELSE 'not_in_customerio'
    END AS email_reachability,

    -- Registration date
    i.user_registered AS inc42_registered_at,

    CURRENT_TIMESTAMP() AS updated_at

FROM unified u
LEFT JOIN inc42 i ON u.primary_email = i.email
LEFT JOIN customerio c ON u.primary_email = c.email
LEFT JOIN woo w ON u.primary_email = w.email
LEFT JOIN tally t ON u.primary_email = t.email

-- Exclude test/junk contacts
WHERE u.primary_email NOT LIKE '%test%'
  AND u.primary_email NOT LIKE '%testing%'
  AND u.primary_email NOT LIKE '%example.com'
  AND u.primary_email NOT LIKE '%mailinator%'
  AND COALESCE(u.first_name, '') NOT LIKE '%test%'
  AND u.primary_email IS NOT NULL
