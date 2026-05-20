{# ──────────────────────────────────────────────────────────────────
   event_franchise: collapse free-text event names into franchise families.
   Decisions: D2CX Converge is its own franchise (NOT under D2C Summit);
   BigShift is its own franchise (NOT under FAST42).
   ────────────────────────────────────────────────────────────────── #}
{% macro event_franchise(col) %}
    CASE
        WHEN {{ col }} IS NULL OR TRIM({{ col }}) = '' THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'd2cx[ _-]?converge') THEN 'D2CX Converge'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'd2c[ _-]?(summit|day|retreat|&[ _-]?retail|whatsapp videos)|d2c summit|the d2c') THEN 'D2C Summit'
        -- GenAI Summit franchise (rebranded to "Inc42 AI Summit" in 2026, but
        -- continuation of the same event series, so we keep one franchise).
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'genai|gen ai|inc42 ai summit|ai summit by inc42|ai-summit|startup leaders pass') THEN 'GenAI Summit'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'fintech[ _-]?summit|fintech') THEN 'Fintech Summit'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bfast[ _-]?42\b') THEN 'FAST42'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'bigshift|big[ _-]shift') THEN 'BigShift'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'ai workshop') THEN 'AI Workshop'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'inc42 show|inc42 ama|\bama registration') THEN 'Inc42 Show'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'inc42 brandlabs|brand[ _-]?labs') THEN 'Inc42 BrandLabs'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'moneyx|money x') THEN 'MoneyX'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'startup submissions|startup spotlight|startup deals|startup program') THEN 'Startup Programs'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'webinar|niti aayog|100x vc|covid') THEN 'Webinars'
        -- Standalone D2C-exclusive pass SKUs (no event prefix in name).
        -- These names + pricing are exclusive to D2C Summit at Inc42:
        --   "Summit Pass" / "Summit + Workshop Pass" — D2C Summit 3.0 (2025) combo passes
        --   "Select / Enabler / Transfer / Investor / All Access / Growth Pass" — historical D2C tiers
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'^(summit pass|summit \+ workshop pass|select|enabler|transfer|investor|all access|growth|team (select|enabler|investor|all access|growth))( pass| pass\s*-)?\b') THEN 'D2C Summit'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'summit') THEN 'Other Summits'
        ELSE 'Other Events'
    END
{% endmacro %}


{# event_format: summit / workshop / webinar / program / retreat / conclave / show / day #}
{% macro event_format(col) %}
    CASE
        WHEN {{ col }} IS NULL OR TRIM({{ col }}) = '' THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bworkshop\b') THEN 'workshop'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bwebinar\b|\bama\b') THEN 'webinar'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bretreat\b') THEN 'retreat'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bconclave\b') THEN 'conclave'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bshow\b') THEN 'show'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bsubmissions?\b|\bapplication\b|\bspotlight\b|\bprogram\b|fast[ _-]?42|bigshift|brandlabs') THEN 'program'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'd2c[ _-]?day') THEN 'summit'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bsummit\b|d2cx converge') THEN 'summit'
        ELSE 'event'
    END
{% endmacro %}


{# event_edition: extract year/version/suffix from event name #}
{% macro event_edition(col) %}
    CASE
        WHEN REGEXP_CONTAINS({{ col }}, r'\b(20\d{2})\b') THEN REGEXP_EXTRACT({{ col }}, r'\b(20\d{2})\b')
        WHEN REGEXP_CONTAINS({{ col }}, r'\b\d+(?:\.\d+)\b') THEN REGEXP_EXTRACT({{ col }}, r'(\d+(?:\.\d+))')
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\b(\d+)(?:st|nd|rd|th)\s*edition\b') THEN REGEXP_EXTRACT(LOWER({{ col }}), r'\b(\d+)(?:st|nd|rd|th)\s*edition\b')
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'season\s*(\d+)') THEN CONCAT('Season ', REGEXP_EXTRACT(LOWER({{ col }}), r'season\s*(\d+)'))
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'hyderabad') THEN 'Hyderabad'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'delhi[ _]ncr|\bdelhi\b') THEN 'Delhi NCR'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'bangalore|bengaluru') THEN 'Bengaluru'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'mumbai') THEN 'Mumbai'
        ELSE NULL
    END
{% endmacro %}


{# event_role: attendee / speaker / sponsor / applicant / registrant
   Default = registrant for form submissions; attendee for paid orders. #}
{% macro event_role(col) %}
    CASE
        WHEN {{ col }} IS NULL OR TRIM({{ col }}) = '' THEN 'registrant'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bsponsor\b') THEN 'sponsor'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bspeaker\b') THEN 'speaker'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bapplication\b|\bsubmissions?\b|application form|onboarding form') THEN 'applicant'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bagenda\b|experiences form') THEN 'attendee'
        ELSE 'registrant'
    END
{% endmacro %}


{# pass_tier: classify paid pass types into a tier hierarchy
   (All Access > Growth > Live > Select > Enabler > Investor > Transfer). #}
{% macro pass_tier(col) %}
    CASE
        WHEN {{ col }} IS NULL OR TRIM({{ col }}) = '' THEN NULL
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'all[ _-]?access') THEN 'All Access'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bgrowth\b') THEN 'Growth'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\blive\b') THEN 'Live'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\bselect\b') THEN 'Select'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\benabler\b') THEN 'Enabler'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\binvestor\b') THEN 'Investor'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'\btransfer\b') THEN 'Transfer'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'startup leaders|leader pass') THEN 'Startup Leaders'
        WHEN REGEXP_CONTAINS(LOWER({{ col }}), r'summit \+ workshop|workshop pass') THEN 'Summit + Workshop'
        ELSE INITCAP(TRIM({{ col }}))
    END
{% endmacro %}
