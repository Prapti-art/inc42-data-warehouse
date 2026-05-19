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
   normalize_city: canonical Indian city names (and title-case rest)
   ────────────────────────────────────────────────────────────────── #}
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
