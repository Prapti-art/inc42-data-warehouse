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

    -- Source coverage
    c.source_count,
    c.found_in_systems,
    c.all_emails,
    c.all_phones,
    c.inc42_registered_at

FROM {{ ref('contacts') }} c
