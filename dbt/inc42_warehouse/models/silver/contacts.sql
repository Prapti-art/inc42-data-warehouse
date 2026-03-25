-- Silver contacts: one row per person with best-available fields from all sources
-- Priority: HubSpot (sales-verified) > Inc42 > Customer.io > WooCommerce > Gravity > Tally

WITH unified AS (
    SELECT * FROM {{ source('silver', 'unified_contacts') }}
),

hubspot AS (
    SELECT LOWER(TRIM(email)) AS email, first_name, last_name, phone,
           company_name, designation, seniority, linkedin_url, city,
           lifecycle_stage, lead_status, hubspot_score, hubspot_owner
    FROM {{ source('bronze', 'hubspot_contacts') }}
),

inc42 AS (
    SELECT LOWER(TRIM(email)) AS email, first_name, last_name, mobile_number AS phone,
           company_name, designation, linkedin_profile_url AS linkedin_url, city,
           user_type, plus_membership_type, plus_start_date, plus_expiry_date,
           daily_newsletter_status, weekly_newsletter_status
    FROM {{ source('bronze', 'inc42_registered_users') }}
),

customerio AS (
    SELECT
        LOWER(TRIM(JSON_VALUE(traits, '$.email'))) AS email,
        JSON_VALUE(traits, '$.first_name') AS first_name,
        JSON_VALUE(traits, '$.last_name') AS last_name,
        JSON_VALUE(traits, '$.phone') AS phone,
        JSON_VALUE(traits, '$.company_name') AS company_name,
        JSON_VALUE(traits, '$.designation') AS designation,
        JSON_VALUE(traits, '$.seniority') AS seniority,
        JSON_VALUE(traits, '$.daily_newsletter_status') AS daily_newsletter,
        JSON_VALUE(traits, '$.weekly_newsletter_status') AS weekly_newsletter,
        JSON_VALUE(traits, '$.ai_shift_newsletter_status') AS ai_shift_newsletter,
        JSON_VALUE(traits, '$.indepth_newsletter_status') AS indepth_newsletter,
        JSON_VALUE(traits, '$.theoutline_newsletter_status') AS theoutline_newsletter,
        JSON_VALUE(traits, '$.markets_newsletter_status') AS markets_newsletter,
        JSON_VALUE(traits, '$.plus_membership_type') AS plus_membership_type,
        JSON_VALUE(traits, '$.plus_cancellation_state') AS plus_cancellation_state,
        JSON_VALUE(traits, '$.engagement_status') AS engagement_status,
        CAST(JSON_VALUE(traits, '$.ltv') AS NUMERIC) AS ltv,
        CAST(JSON_VALUE(traits, '$.sessions') AS INT64) AS sessions,
        JSON_VALUE(traits, '$.email_opt_in') AS email_opt_in,
        JSON_VALUE(traits, '$.whatsapp_subscription') AS whatsapp_opt_in
    FROM {{ source('bronze', 'customerio_identify') }}
),

woo AS (
    SELECT
        email, first_name, last_name, phone, company_name, city, state, country
    FROM (
        SELECT
            LOWER(TRIM(billing_email)) AS email,
            billing_first_name AS first_name,
            billing_last_name AS last_name,
            billing_phone AS phone,
            billing_company AS company_name,
            billing_city AS city,
            billing_state AS state,
            billing_country AS country,
            ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(billing_email)) ORDER BY order_date DESC) AS rn
        FROM {{ source('bronze', 'woocommerce_orders') }}
    )
    WHERE rn = 1
)

SELECT
    u.unified_contact_id,
    u.primary_email AS email,

    -- Name: HubSpot > Inc42 > Customer.io > WooCommerce
    COALESCE(h.first_name, i.first_name, c.first_name, w.first_name) AS first_name,
    COALESCE(h.last_name, i.last_name, c.last_name, w.last_name) AS last_name,

    -- Phone: already normalized by PySpark
    u.primary_phone AS phone,

    -- Company
    COALESCE(h.company_name, i.company_name, c.company_name, w.company_name) AS company_name,

    -- Professional
    COALESCE(h.designation, i.designation, c.designation) AS designation,
    COALESCE(h.seniority, c.seniority) AS seniority,
    COALESCE(h.linkedin_url, i.linkedin_url) AS linkedin_url,

    -- Location
    COALESCE(h.city, i.city, w.city) AS city,
    w.state,
    w.country,

    -- User type
    COALESCE(i.user_type, 'unknown') AS user_type,

    -- Plus membership
    COALESCE(i.plus_membership_type, c.plus_membership_type) AS plus_membership_type,
    i.plus_start_date,
    i.plus_expiry_date,
    c.plus_cancellation_state,
    CASE
        WHEN i.plus_expiry_date IS NOT NULL
        THEN DATE_DIFF(i.plus_expiry_date, CURRENT_DATE(), DAY)
        ELSE NULL
    END AS plus_days_to_expiry,
    CASE
        WHEN c.plus_cancellation_state = 'cancelled' AND i.plus_expiry_date > CURRENT_DATE()
        THEN 'active_cancelling'
        WHEN c.plus_cancellation_state = 'cancelled' AND (i.plus_expiry_date <= CURRENT_DATE() OR i.plus_expiry_date IS NULL)
        THEN 'churned'
        WHEN i.plus_membership_type IS NOT NULL
        THEN 'active'
        ELSE NULL
    END AS plus_status,

    -- Newsletters
    COALESCE(c.daily_newsletter, i.daily_newsletter_status) AS daily_newsletter,
    COALESCE(c.weekly_newsletter, i.weekly_newsletter_status) AS weekly_newsletter,
    c.ai_shift_newsletter,
    c.indepth_newsletter,
    c.theoutline_newsletter,
    c.markets_newsletter,

    -- Engagement
    c.engagement_status,
    COALESCE(c.ltv, 0) AS ltv,
    COALESCE(c.sessions, 0) AS sessions,

    -- Channel reachability
    c.email_opt_in,
    c.whatsapp_opt_in,

    -- Lead (HubSpot)
    h.lifecycle_stage,
    h.lead_status,
    h.hubspot_score,
    h.hubspot_owner,

    -- Source count
    u.source_count,
    u.found_in_systems,

    CURRENT_TIMESTAMP() AS updated_at

FROM unified u
LEFT JOIN hubspot h ON u.primary_email = h.email
LEFT JOIN inc42 i ON u.primary_email = i.email
LEFT JOIN customerio c ON u.primary_email = c.email
LEFT JOIN woo w ON u.primary_email = w.email
