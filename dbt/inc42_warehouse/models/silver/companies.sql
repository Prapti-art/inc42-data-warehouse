-- Silver companies: Datalabs company_table enriched with funding, financials, employees

WITH company AS (
    SELECT
        company_uuid AS company_id,
        name AS company_name,
        website,
        REPLACE(REPLACE(website, 'https://', ''), 'http://', '') AS domain,
        sector,
        sub_sector,
        business_model,
        city,
        state,
        country,
        founded_date AS founded_year,
        tags,
        linkedin,
        slug
    FROM {{ source('bronze', 'dl_company_table') }}
    WHERE name IS NOT NULL
),

-- Latest funding round per company
funding AS (
    SELECT
        company_uuid,
        SUM(amount_raised_in_usd) AS total_funding_usd,
        MAX(funding_date) AS last_funding_date,
        MAX(CASE WHEN funding_date = max_date THEN funding_stage END) AS last_funding_stage,
        MAX(CASE WHEN funding_date = max_date THEN funding_type END) AS last_funding_type,
        COUNT(*) AS total_funding_rounds
    FROM (
        SELECT *,
            MAX(funding_date) OVER (PARTITION BY company_uuid) AS max_date
        FROM {{ source('bronze', 'dl_funding_table') }}
        WHERE amount_raised_in_usd IS NOT NULL
    )
    GROUP BY company_uuid
),

-- Latest P&L per company (prefer Consolidated, fallback to Standalone)
financials AS (
    SELECT company_uuid, revenue_from_operations, total_revenue,
           profit_loss_for_the_period, total_expenses, as_of_date, financial_type
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY company_uuid
                ORDER BY
                    CASE WHEN financial_type = 'Consolidated' THEN 0 ELSE 1 END,
                    as_of_date DESC
            ) AS rn
        FROM {{ source('bronze', 'dl_profit_loss_table') }}
    )
    WHERE rn = 1
),

-- Latest employee count per company
employees AS (
    SELECT company_uuid, employee_count_number AS employee_count, as_of_date AS employee_as_of
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY company_uuid ORDER BY as_of_date DESC) AS rn
        FROM {{ source('bronze', 'dl_employee_trendline') }}
        WHERE employee_count_number IS NOT NULL
    )
    WHERE rn = 1
),

-- Latest web analytics per company
web AS (
    SELECT company_uuid, monthly_visits, bounce_rate, visit_duration, pages_per_visit
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY company_uuid ORDER BY as_of_date DESC) AS rn
        FROM {{ source('bronze', 'dl_website_analytics_table') }}
    )
    WHERE rn = 1
)

SELECT
    c.company_id,
    c.company_name,
    c.website,
    c.domain,
    c.sector,
    c.sub_sector,
    c.business_model,
    c.city,
    c.state,
    c.country,
    c.founded_year,
    c.tags,
    c.linkedin,

    -- Funding
    COALESCE(f.total_funding_usd, 0) AS total_funding_usd,
    f.last_funding_date,
    f.last_funding_stage,
    f.last_funding_type,
    COALESCE(f.total_funding_rounds, 0) AS total_funding_rounds,

    -- Financials (latest P&L)
    fin.revenue_from_operations,
    fin.total_revenue,
    fin.profit_loss_for_the_period,
    fin.total_expenses,
    CASE WHEN fin.profit_loss_for_the_period > 0 THEN TRUE ELSE FALSE END AS is_profitable,
    fin.as_of_date AS financials_as_of,

    -- Employees
    e.employee_count,
    e.employee_as_of,

    -- Web traffic
    w.monthly_visits,
    w.bounce_rate,
    w.visit_duration,
    w.pages_per_visit,

    CURRENT_TIMESTAMP() AS updated_at

FROM company c
LEFT JOIN funding f ON c.company_id = f.company_uuid
LEFT JOIN financials fin ON c.company_id = fin.company_uuid
LEFT JOIN employees e ON c.company_id = e.company_uuid
LEFT JOIN web w ON c.company_id = w.company_uuid
