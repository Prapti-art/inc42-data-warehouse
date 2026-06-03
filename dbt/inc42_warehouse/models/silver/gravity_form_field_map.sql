-- silver.gravity_form_field_map
-- ONE ROW per (form_id, field_id) parsed from 42_gf_form_meta.display_meta JSON.
-- field_id is a STRING because compound fields (name, address) use sub-ids like '1.3', '1.6'.
-- Maps each Gravity Forms field to a canonical name so downstream extracts can JOIN
-- on (form_id, field_id) instead of guessing via global meta_key indices.
-- Replaces the meta_key=10 collision that put 'source' / 'Lead Source' values into
-- company_name for 23k+ contacts.

WITH all_fields AS (
    -- Top-level fields (id is INT in JSON, stored as STRING here for uniform join)
    SELECT
        SAFE_CAST(form_id AS INT64) AS form_id,
        CAST(JSON_VALUE(f, '$.id') AS STRING)        AS field_id,
        JSON_VALUE(f, '$.type')                      AS field_type,
        JSON_VALUE(f, '$.label')                     AS field_label,
        JSON_VALUE(f, '$.adminLabel')                AS admin_label,
        JSON_VALUE(f, '$.inputName')                 AS input_name,
        CAST(NULL AS STRING)                         AS parent_field_type
    FROM {{ source('bronze', 'gravity_forms_form_meta') }},
         UNNEST(JSON_QUERY_ARRAY(display_meta, '$.fields')) AS f

    UNION ALL

    -- Sub-fields (inputs[]) for compound fields like name/address (id='1.3', '1.6', '8.4' etc.)
    SELECT
        SAFE_CAST(form_id AS INT64) AS form_id,
        JSON_VALUE(i, '$.id')                        AS field_id,
        -- inherit type from parent + add the input-specific label
        JSON_VALUE(f, '$.type')                      AS field_type,
        JSON_VALUE(i, '$.label')                     AS field_label,
        JSON_VALUE(i, '$.customLabel')               AS admin_label,
        JSON_VALUE(i, '$.name')                      AS input_name,
        JSON_VALUE(f, '$.type')                      AS parent_field_type
    FROM {{ source('bronze', 'gravity_forms_form_meta') }},
         UNNEST(JSON_QUERY_ARRAY(display_meta, '$.fields')) AS f,
         UNNEST(IFNULL(JSON_QUERY_ARRAY(f, '$.inputs'), [])) AS i
),

parsed AS (
    SELECT *,
        LOWER(COALESCE(admin_label, field_label, input_name, '')) AS label_lc
    FROM all_fields
),

mapped AS (
    SELECT *,
        CASE
            -- ── EMAIL ──
            WHEN field_type = 'email' AND parent_field_type IS NULL THEN 'email'
            WHEN REGEXP_CONTAINS(label_lc, r'\b(work\s*)?(e-?mail|email\s*id|email\s*address|business\s*email|primary\s*email)\b') THEN 'email'

            -- ── NAME compound: sub-fields ──
            -- Standard Gravity Forms 'name' sub-input ids:
            --   1.2 = Prefix, 1.3 = First, 1.4 = Middle, 1.6 = Last, 1.8 = Suffix
            WHEN parent_field_type = 'name' AND REGEXP_CONTAINS(field_id, r'\.3$') THEN 'first_name'
            WHEN parent_field_type = 'name' AND REGEXP_CONTAINS(field_id, r'\.6$') THEN 'last_name'
            WHEN parent_field_type = 'name' AND REGEXP_CONTAINS(label_lc, r'^first') THEN 'first_name'
            WHEN parent_field_type = 'name' AND REGEXP_CONTAINS(label_lc, r'^last|surname|family') THEN 'last_name'

            -- ── NAME top-level (single text field labeled name) ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(first[ _-]?name|given[ _-]?name|fname)\b') THEN 'first_name'
            WHEN REGEXP_CONTAINS(label_lc, r'^(last[ _-]?name|surname|family[ _-]?name|lname)\b') THEN 'last_name'
            WHEN field_type = 'name' AND parent_field_type IS NULL THEN 'full_name'
            WHEN REGEXP_CONTAINS(label_lc, r'^(full[ _-]?name|your[ _-]?name|name)$') THEN 'full_name'

            -- ── PHONE ──
            WHEN field_type = 'phone' AND parent_field_type IS NULL THEN 'phone'
            WHEN REGEXP_CONTAINS(label_lc, r'\b(phone|mobile|contact[ _-]?number|cell|whatsapp[ _-]?(no|number)|mobile[ _-]?no|phone[ _-]?(no|number))\b') THEN 'phone'

            -- ── COMPANY ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(company[ _-]?name|company|organi[sz]ation([ _-]?name)?|employer|firm([ _-]?name)?|startup([ _-]?name)?|business([ _-]?name)?|name[ _-]?of[ _-]?(company|organi[sz]ation|firm|startup))\b') THEN 'company_name'
            WHEN REGEXP_CONTAINS(label_lc, r'where\s+do\s+you\s+work') THEN 'company_name'

            -- ── DESIGNATION / TITLE ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(designation|job[ _-]?title|title|role|position|your[ _-]?role|current[ _-]?(role|designation)|current[ _-]?position)\b') THEN 'designation'

            -- ── SENIORITY ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(seniority|seniority[ _-]?level|level|grade|hierarchy)\b') THEN 'seniority'

            -- ── LINKEDIN ──
            WHEN REGEXP_CONTAINS(label_lc, r'linkedin') THEN 'linkedin_url'

            -- ── ADDRESS compound: sub-fields (id.3=city, id.4=state, id.6=country) ──
            WHEN parent_field_type = 'address' AND REGEXP_CONTAINS(field_id, r'\.3$') THEN 'city'
            WHEN parent_field_type = 'address' AND REGEXP_CONTAINS(field_id, r'\.4$') THEN 'state'
            WHEN parent_field_type = 'address' AND REGEXP_CONTAINS(field_id, r'\.6$') THEN 'country'

            -- ── LOCATION (standalone single-text fields) ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(city|town|location)\b') THEN 'city'
            WHEN REGEXP_CONTAINS(label_lc, r'^(state|province)\b') THEN 'state'
            WHEN REGEXP_CONTAINS(label_lc, r'^country\b') THEN 'country'

            -- ── INDUSTRY ──
            WHEN REGEXP_CONTAINS(label_lc, r'^(industry|sector|vertical)\b') THEN 'industry'

            -- ── LEAD SOURCE / ATTRIBUTION (explicitly NOT company_name) ──
            -- These are the fields that polluted company_name with 'source'/'guest post'/etc.
            WHEN REGEXP_CONTAINS(label_lc, r'(how\s+did\s+you\s+hear|where\s+did\s+you\s+(hear|find|come)|^source$|referr?al|utm|tracking)') THEN 'lead_source'

            ELSE NULL
        END AS canonical_field
    FROM parsed
)

SELECT
    form_id,
    field_id,
    field_type,
    parent_field_type,
    field_label,
    admin_label,
    input_name,
    canonical_field,
    CURRENT_TIMESTAMP() AS updated_at
FROM mapped
WHERE field_id IS NOT NULL
