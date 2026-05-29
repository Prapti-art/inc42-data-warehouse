{# ──────────────────────────────────────────────────────────────────
   scrub_sentinel: NULL-out junk values
   - default: scrubs exact junk strings only (safe for names)
   - allow_short=false: also scrubs anything with length <= 2 (safer for
     non-name fields like company/designation/city/state/country where
     "x" / "." / "-" are never real)
   ────────────────────────────────────────────────────────────────── #}
{% macro scrub_sentinel(col, allow_short=true) %}
    CASE
        WHEN TRIM(LOWER({{ col }})) IN (
            'undefined','na','n/a','n.a','n.a.','null','none','none.','nan',
            'test','testing','tbd','xxx','xx','asdf','abc','-na-',
            'not applicable','not available','not specified','none of the above',
            '-','--','---','_','__','.','..','...','/','\\\\','?','??','*'
        ) THEN NULL
        {% if not allow_short %}
        WHEN LENGTH(TRIM({{ col }})) <= 2 THEN NULL
        {% endif %}
        WHEN TRIM({{ col }}) = '' THEN NULL
        ELSE TRIM({{ col }})
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   title_case_clean: trims, collapses whitespace, NULL-scrubs, title-cases.
   Used for names, cities, companies. Preserves all-caps acronyms in
   companies/titles is hard in pure SQL — this is good-enough title case.
   ────────────────────────────────────────────────────────────────── #}
{% macro title_case_clean(col, allow_short=true) %}
    INITCAP(
        REGEXP_REPLACE(
            TRIM({{ scrub_sentinel(col, allow_short) }}),
            r'\s+', ' '
        )
    )
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   normalize_country: ISO codes + common variants -> canonical country name
   ────────────────────────────────────────────────────────────────── #}
{% macro normalize_country(col) %}
    CASE UPPER(TRIM({{ scrub_sentinel(col) }}))
        WHEN 'IN' THEN 'India' WHEN 'IND' THEN 'India' WHEN 'INDIA' THEN 'India'
        WHEN 'US' THEN 'United States' WHEN 'USA' THEN 'United States'
        WHEN 'U.S.' THEN 'United States' WHEN 'U.S.A.' THEN 'United States'
        WHEN 'UNITED STATES' THEN 'United States'
        WHEN 'UNITED STATES OF AMERICA' THEN 'United States'
        WHEN 'GB' THEN 'United Kingdom' WHEN 'UK' THEN 'United Kingdom'
        WHEN 'U.K.' THEN 'United Kingdom' WHEN 'GREAT BRITAIN' THEN 'United Kingdom'
        WHEN 'ENGLAND' THEN 'United Kingdom' WHEN 'UNITED KINGDOM' THEN 'United Kingdom'
        WHEN 'AE' THEN 'United Arab Emirates' WHEN 'UAE' THEN 'United Arab Emirates'
        WHEN 'U.A.E.' THEN 'United Arab Emirates'
        WHEN 'UNITED ARAB EMIRATES' THEN 'United Arab Emirates'
        WHEN 'SG' THEN 'Singapore' WHEN 'SGP' THEN 'Singapore'
        WHEN 'BD' THEN 'Bangladesh' WHEN 'BGD' THEN 'Bangladesh'
        WHEN 'PK' THEN 'Pakistan' WHEN 'PAK' THEN 'Pakistan'
        WHEN 'JP' THEN 'Japan' WHEN 'JPN' THEN 'Japan'
        WHEN 'DE' THEN 'Germany' WHEN 'DEU' THEN 'Germany'
        WHEN 'FR' THEN 'France' WHEN 'FRA' THEN 'France'
        WHEN 'AU' THEN 'Australia' WHEN 'AUS' THEN 'Australia'
        WHEN 'CA' THEN 'Canada' WHEN 'CAN' THEN 'Canada'
        WHEN 'CN' THEN 'China' WHEN 'CHN' THEN 'China'
        WHEN 'HK' THEN 'Hong Kong' WHEN 'HKG' THEN 'Hong Kong'
        WHEN 'MY' THEN 'Malaysia' WHEN 'MYS' THEN 'Malaysia'
        WHEN 'TH' THEN 'Thailand' WHEN 'THA' THEN 'Thailand'
        WHEN 'ID' THEN 'Indonesia' WHEN 'IDN' THEN 'Indonesia'
        WHEN 'PH' THEN 'Philippines' WHEN 'PHL' THEN 'Philippines'
        WHEN 'VN' THEN 'Vietnam' WHEN 'VNM' THEN 'Vietnam'
        WHEN 'KR' THEN 'South Korea' WHEN 'KOR' THEN 'South Korea'
        WHEN 'NP' THEN 'Nepal' WHEN 'NPL' THEN 'Nepal'
        WHEN 'LK' THEN 'Sri Lanka' WHEN 'LKA' THEN 'Sri Lanka'
        WHEN 'SA' THEN 'Saudi Arabia' WHEN 'SAU' THEN 'Saudi Arabia'
        WHEN 'NG' THEN 'Nigeria' WHEN 'NGA' THEN 'Nigeria'
        WHEN 'BR' THEN 'Brazil' WHEN 'BRA' THEN 'Brazil'
        WHEN 'NL' THEN 'Netherlands' WHEN 'NLD' THEN 'Netherlands'
        WHEN 'CH' THEN 'Switzerland' WHEN 'CHE' THEN 'Switzerland'
        WHEN 'SE' THEN 'Sweden' WHEN 'SWE' THEN 'Sweden'
        ELSE INITCAP(LOWER(TRIM({{ scrub_sentinel(col) }})))
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   normalize_state: Indian state codes -> full name.
   Falls back to title case for non-matching values (US states etc.).
   ────────────────────────────────────────────────────────────────── #}
{% macro normalize_state(col) %}
    CASE UPPER(TRIM({{ scrub_sentinel(col) }}))
        WHEN 'AP' THEN 'Andhra Pradesh' WHEN 'AR' THEN 'Arunachal Pradesh'
        WHEN 'AS' THEN 'Assam' WHEN 'BR' THEN 'Bihar'
        WHEN 'CT' THEN 'Chhattisgarh' WHEN 'CG' THEN 'Chhattisgarh'
        WHEN 'GA' THEN 'Goa' WHEN 'GJ' THEN 'Gujarat'
        WHEN 'HR' THEN 'Haryana' WHEN 'HP' THEN 'Himachal Pradesh'
        WHEN 'JH' THEN 'Jharkhand' WHEN 'KA' THEN 'Karnataka'
        WHEN 'KL' THEN 'Kerala' WHEN 'MP' THEN 'Madhya Pradesh'
        WHEN 'MH' THEN 'Maharashtra' WHEN 'MN' THEN 'Manipur'
        WHEN 'ML' THEN 'Meghalaya' WHEN 'MZ' THEN 'Mizoram'
        WHEN 'NL' THEN 'Nagaland' WHEN 'OR' THEN 'Odisha'
        WHEN 'OD' THEN 'Odisha' WHEN 'PB' THEN 'Punjab'
        WHEN 'RJ' THEN 'Rajasthan' WHEN 'SK' THEN 'Sikkim'
        WHEN 'TN' THEN 'Tamil Nadu' WHEN 'TS' THEN 'Telangana'
        WHEN 'TG' THEN 'Telangana' WHEN 'TR' THEN 'Tripura'
        WHEN 'UP' THEN 'Uttar Pradesh' WHEN 'UT' THEN 'Uttarakhand'
        WHEN 'UK' THEN 'Uttarakhand' WHEN 'WB' THEN 'West Bengal'
        WHEN 'DL' THEN 'Delhi' WHEN 'JK' THEN 'Jammu and Kashmir'
        WHEN 'LA' THEN 'Ladakh' WHEN 'CH' THEN 'Chandigarh'
        WHEN 'AN' THEN 'Andaman and Nicobar Islands'
        WHEN 'DN' THEN 'Dadra and Nagar Haveli and Daman and Diu'
        WHEN 'DD' THEN 'Dadra and Nagar Haveli and Daman and Diu'
        WHEN 'LD' THEN 'Lakshadweep' WHEN 'PY' THEN 'Puducherry'
        WHEN 'NCR' THEN 'Delhi' WHEN 'NCT OF DELHI' THEN 'Delhi'
        WHEN 'NEW DELHI' THEN 'Delhi'
        ELSE INITCAP(LOWER(TRIM({{ scrub_sentinel(col) }})))
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   normalize_seniority: collapse the long tail of seniority labels
   into a fixed bucket set: Founder, C-Suite, VP/Director, Manager,
   Senior, Associate, Junior, Intern, Student, Investor, Partner,
   Researcher, Consultant. Garbage / "Other" -> NULL.
   ────────────────────────────────────────────────────────────────── #}
{% macro normalize_seniority(col) %}
    -- Closed enum: only these 15 values are emitted. Anything else -> NULL.
    --   Founder, C-Suite, VP/Director, Senior, Manager, Associate, Junior,
    --   Intern, Student, Investor, Partner, Researcher, Consultant,
    --   Government, Startup Enthusiast
    CASE
        WHEN {{ scrub_sentinel(col) }} IS NULL THEN NULL
        WHEN LOWER(TRIM({{ col }})) IN (
            'other','others','none','n/a','na','yes','no','high','top','nn','c','md','management'
        ) THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'^\d+([.,]\d+)?(\s*(years?|yrs))?\s*$') THEN NULL
        WHEN REGEXP_CONTAINS(TRIM({{ col }}), r'^\[.*\]$') THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'founder|owner|promoter') THEN 'Founder'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bceo\b|\bcfo\b|\bcto\b|\bcoo\b|\bcmo\b|\bcpo\b|\bchro\b|\bcxo\b|\bcxos\b|c-?suite|c-?level|chief|president\b|leadership|top[ _-]?management|senior[ _-]?management|senior[ _-]?leadership') THEN 'C-Suite'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bvp\b|vpsenior|vice president|director|general manager|\bgm\b|head of|head -') THEN 'VP/Director'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'senior manager|sr\.? manager|senior_associate') THEN 'Senior'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'manager|middle[ _-]?management|mid[ _-]?level[ _-]?management') THEN 'Manager'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'analyst|associate') THEN 'Associate'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'junior|jr\.?|trainee|fresher|entry[ _-]?level') THEN 'Junior'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'intern\b|internship') THEN 'Intern'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'student|undergrad|graduate student') THEN 'Student'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'investor|venture|angel') THEN 'Investor'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bpartner\b') THEN 'Partner'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'researcher|academic|journalist|professor|faculty') THEN 'Researcher'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'consultant|advisor|advisory') THEN 'Consultant'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'government|policy[ _-]?maker') THEN 'Government'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'startup enthusiast|enthusiast') THEN 'Startup Enthusiast'
        ELSE NULL
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   normalize_job_function: collapse function labels into a fixed set:
   Engineering, Product, Design, Data, Marketing, Sales & BD,
   Operations, Finance & Accounting, HR, Legal & Compliance,
   Consulting, Research, Founders Office, R&D.
   "Others" -> NULL.
   ────────────────────────────────────────────────────────────────── #}
{% macro normalize_job_function(col) %}
    CASE
        WHEN {{ scrub_sentinel(col) }} IS NULL THEN NULL
        WHEN LOWER(TRIM({{ col }})) IN ('other','others','none','n/a','na') THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'engineering|engineer\b|software|developer') THEN 'Engineering'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'product\b') THEN 'Product'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'design\b|\bux\b|\bui\b') THEN 'Design'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bdata\b|analytics|machine learning|\bai\b|\bml\b') THEN 'Data'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'marketing|growth|brand') THEN 'Marketing'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'sales|business[ _]development|\bbd\b|biz[ _]dev') THEN 'Sales & BD'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'finance|accounting|\bfp&a\b|\bfpa\b|treasury') THEN 'Finance & Accounting'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'human resources|\bhr\b|talent|people|l&d|lnd') THEN 'HR'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'legal|compliance|secretarial') THEN 'Legal & Compliance'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'consult') THEN 'Consulting'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'research') THEN 'Research'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'founder.?s? office|chief of staff') THEN 'Founders Office'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\br&d\b|\brd\b|r and d') THEN 'R&D'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'operations|\bops\b|management') THEN 'Operations'
        ELSE INITCAP(REGEXP_REPLACE(LOWER(TRIM({{ col }})), r'[_-]+', ' '))
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────────────
   normalize_designation: clean + title-case, but preserve common
   business acronyms (CEO, CTO, CFO, COO, CMO, CXO, VP, MD, GM, HR,
   PR, IT, BD, ML, AI, UX, UI, SDE, QA, KAM, PM). Also canonicalizes
   founder/CEO variants.
   ────────────────────────────────────────────────────────────────── #}
{% macro normalize_designation(col) %}
    CASE
        WHEN {{ scrub_sentinel(col, allow_short=false) }} IS NULL THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(co[ -]?founder)( & ?| and )(ceo)$') THEN 'Co-Founder & CEO'
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^co[ -]?founder$') THEN 'Co-Founder'
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(founder)( & ?| and |/)(ceo)$') THEN 'Founder & CEO'
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^founder$') THEN 'Founder'
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^chief executive officer$') THEN 'CEO'
        WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^chief (financial|technology|operating|marketing|product|people|human resources) officer$')
            THEN CASE LOWER(REGEXP_EXTRACT({{ col }}, r'(?i)Chief\s+(\w+(?:\s+\w+)?)\s+Officer'))
                WHEN 'financial' THEN 'CFO'
                WHEN 'technology' THEN 'CTO'
                WHEN 'operating' THEN 'COO'
                WHEN 'marketing' THEN 'CMO'
                WHEN 'product' THEN 'CPO'
                WHEN 'people' THEN 'CHRO'
                WHEN 'human resources' THEN 'CHRO'
                ELSE INITCAP(TRIM({{ col }}))
            END
        ELSE {{ uppercase_acronyms('INITCAP(REGEXP_REPLACE(TRIM(' ~ col ~ "), r'\\s+', ' '))") }}
    END
{% endmacro %}


{# Helper: re-uppercase common business acronyms after INITCAP.
   BigQuery REGEXP_REPLACE has no callback, so we chain literal replacements. #}
{% macro uppercase_acronyms(expr) %}
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
        {{ expr }},
        r'\bCeo\b','CEO'), r'\bCto\b','CTO'), r'\bCfo\b','CFO'), r'\bCoo\b','COO'), r'\bCmo\b','CMO'),
        r'\bCpo\b','CPO'), r'\bChro\b','CHRO'), r'\bCxo\b','CXO'), r'\bCdo\b','CDO'), r'\bCgo\b','CGO'),
        r'\bVp\b','VP'), r'\bAvp\b','AVP'), r'\bSvp\b','SVP'), r'\bEvp\b','EVP'), r'\bMd\b','MD'),
        r'\bGm\b','GM'), r'\bHr\b','HR'), r'\bPr\b','PR'), r'\bIt\b','IT'), r'\bBd\b','BD'),
        r'\bPm\b','PM'), r'\bKam\b','KAM'), r'\bSde\b','SDE'), r'\bQa\b','QA'), r'\bSme\b','SME'),
        r'\bAi\b','AI'), r'\bMl\b','ML'), r'\bUx\b','UX'), r'\bUi\b','UI'), r'\bCrm\b','CRM'),
        r'\bErp\b','ERP'), r'\bSaas\b','SaaS'), r'\bB2b\b','B2B'), r'\bB2c\b','B2C'), r'\bD2c\b','D2C')
{% endmacro %}
{# ──────────────────────────────────────────────────────────────────
   linkedin_slug: extract the /in/<slug> handle from a LinkedIn URL.
   Works for any subdomain (www. / in. / m. / none). Returns NULL when
   the URL has no /in/ profile path (company pages, /feed, /me, bare
   linkedin.com, facebook links, etc.) — critically, it must NOT collapse
   malformed URLs to a constant like "https:", which previously caused
   hundreds of contacts to false-match one Datalabs person.
   ────────────────────────────────────────────────────────────────── #}
{% macro linkedin_slug(col) %}
    NULLIF(
        REGEXP_EXTRACT(LOWER(TRIM({{ col }})), r'linkedin\.com/in/([a-z0-9\-_%\.]+)'),
        ''
    )
{% endmacro %}


{% macro normalize_city(col) %}
    CASE UPPER(TRIM({{ scrub_sentinel(col) }}))
        WHEN 'BANGALORE' THEN 'Bengaluru' WHEN 'BANGLORE' THEN 'Bengaluru'
        WHEN 'BENGALURU' THEN 'Bengaluru' WHEN 'BLR' THEN 'Bengaluru'
        WHEN 'BOMBAY' THEN 'Mumbai' WHEN 'MUMBAI' THEN 'Mumbai'
        WHEN 'BOM' THEN 'Mumbai'
        WHEN 'CALCUTTA' THEN 'Kolkata' WHEN 'KOLKATA' THEN 'Kolkata'
        WHEN 'CCU' THEN 'Kolkata'
        WHEN 'MADRAS' THEN 'Chennai' WHEN 'CHENNAI' THEN 'Chennai'
        WHEN 'MAA' THEN 'Chennai'
        WHEN 'GURGAON' THEN 'Gurugram' WHEN 'GURUGRAM' THEN 'Gurugram'
        WHEN 'GGN' THEN 'Gurugram'
        WHEN 'POONA' THEN 'Pune' WHEN 'PUNE' THEN 'Pune'
        WHEN 'TRIVANDRUM' THEN 'Thiruvananthapuram'
        WHEN 'THIRUVANANTHAPURAM' THEN 'Thiruvananthapuram'
        WHEN 'VIZAG' THEN 'Visakhapatnam'
        WHEN 'VISAKHAPATNAM' THEN 'Visakhapatnam'
        WHEN 'BARODA' THEN 'Vadodara' WHEN 'VADODARA' THEN 'Vadodara'
        WHEN 'COCHIN' THEN 'Kochi' WHEN 'KOCHI' THEN 'Kochi'
        WHEN 'NOIDA' THEN 'Noida' WHEN 'NEW DELHI' THEN 'New Delhi'
        WHEN 'DELHI' THEN 'Delhi' WHEN 'DEL' THEN 'Delhi'
        WHEN 'HYD' THEN 'Hyderabad' WHEN 'HYDERABAD' THEN 'Hyderabad'
        WHEN 'AHMD' THEN 'Ahmedabad' WHEN 'AHMEDABAD' THEN 'Ahmedabad'
        ELSE INITCAP(LOWER(TRIM({{ scrub_sentinel(col) }})))
    END
{% endmacro %}
