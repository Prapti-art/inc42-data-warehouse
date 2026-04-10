-- Gold dim_contact: master contact dimension with ALL enriched fields

SELECT
    ROW_NUMBER() OVER (ORDER BY c.unified_contact_id) AS contact_key,
    c.*

FROM {{ ref('contacts') }} c
