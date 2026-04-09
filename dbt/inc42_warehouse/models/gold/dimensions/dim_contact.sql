-- Gold dim_contact: master contact dimension (star schema — company denormalized in)

SELECT
    ROW_NUMBER() OVER (ORDER BY c.unified_contact_id) AS contact_key,
    c.unified_contact_id,
    c.email,
    c.first_name,
    c.last_name,
    c.phone,
    c.company_name,

    -- Professional
    c.designation,
    c.seniority,
    c.linkedin_url,
    c.city,
    c.state,
    c.country,
    c.user_type,

    -- Newsletter subscriptions
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

    -- Source coverage
    c.source_count,
    c.found_in_systems,
    c.all_emails,
    c.all_phones,
    c.inc42_registered_at

FROM {{ ref('contacts') }} c
