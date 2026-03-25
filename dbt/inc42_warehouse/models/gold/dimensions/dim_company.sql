-- Gold dim_company: for company-level fact tables (financials, funding, headcount)

SELECT
    ROW_NUMBER() OVER (ORDER BY company_id) AS company_key,
    company_id,
    company_name,
    legal_name,
    website,
    domain,
    founded_year,
    hq_city,
    hq_state,
    sector,
    sub_sector,
    business_model,
    company_status,
    employee_count,
    latest_revenue_inr,
    latest_pat_inr,
    is_profitable,
    net_margin_pct,
    total_funding_inr,
    last_funding_stage,
    last_funding_date,
    key_investors,
    monthly_web_visits,
    app_rating,
    glassdoor_rating

FROM {{ ref('companies') }}
