-- Gold dim_company: company dimension powered by Datalabs (75K+ companies)

SELECT
    ROW_NUMBER() OVER (ORDER BY company_id) AS company_key,
    company_id,
    company_name,
    website,
    domain,
    sector,
    sub_sector,
    business_model,
    city,
    state,
    country,
    founded_year,
    tags,
    linkedin,

    -- Funding
    total_funding_usd,
    last_funding_date,
    last_funding_stage,
    last_funding_type,
    total_funding_rounds,

    -- Financials
    revenue_from_operations,
    total_revenue,
    profit_loss_for_the_period,
    total_expenses,
    is_profitable,
    financials_as_of,

    -- Employees
    employee_count,
    employee_as_of,

    -- Web traffic
    monthly_visits,
    bounce_rate,
    visit_duration,
    pages_per_visit

FROM {{ ref('companies') }}
