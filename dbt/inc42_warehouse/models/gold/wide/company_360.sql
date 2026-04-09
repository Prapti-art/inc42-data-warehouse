-- Gold company_360: ONE ROW = EVERYTHING about a company
-- Datalabs company data + Inc42 engagement metrics

WITH company AS (
    SELECT * FROM {{ ref('dim_company') }}
),

-- How many Inc42 contacts work at each company
inc42_contacts AS (
    SELECT
        LOWER(TRIM(company_name)) AS company_name_lower,
        COUNT(*) AS contacts_in_warehouse,
        COUNT(DISTINCT contact_key) AS unique_contacts
    FROM {{ ref('dim_contact') }}
    WHERE company_name IS NOT NULL AND company_name != ''
    GROUP BY LOWER(TRIM(company_name))
),

-- Revenue from WooCommerce orders linked to company employees
company_orders AS (
    SELECT
        LOWER(TRIM(dc.company_name)) AS company_name_lower,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN fo.is_completed = 1 THEN fo.net_revenue ELSE 0 END) AS total_revenue
    FROM {{ ref('fact_orders') }} fo
    JOIN {{ ref('dim_contact') }} dc ON fo.contact_key = dc.contact_key
    WHERE dc.company_name IS NOT NULL AND dc.company_name != ''
    GROUP BY LOWER(TRIM(dc.company_name))
)

SELECT
    co.company_key,
    co.company_id,
    co.company_name,
    co.website,
    co.domain,
    co.sector,
    co.sub_sector,
    co.business_model,
    co.city,
    co.state,
    co.country,
    co.founded_year,
    co.tags,
    co.linkedin,

    -- Funding
    co.total_funding_usd,
    co.last_funding_date,
    co.last_funding_stage,
    co.last_funding_type,
    co.total_funding_rounds,

    -- Financials
    co.revenue_from_operations,
    co.total_revenue,
    co.profit_loss_for_the_period,
    co.is_profitable,

    -- Employees & Web
    co.employee_count,
    co.monthly_visits,

    -- Inc42 engagement
    COALESCE(ic.contacts_in_warehouse, 0) AS contacts_in_warehouse,
    COALESCE(ic.unique_contacts, 0) AS unique_contacts,
    COALESCE(cor.total_orders, 0) AS total_orders_from_company,
    COALESCE(cor.total_revenue, 0) AS total_revenue_from_company,

    CURRENT_TIMESTAMP() AS updated_at

FROM company co
LEFT JOIN inc42_contacts ic ON LOWER(TRIM(co.company_name)) = ic.company_name_lower
LEFT JOIN company_orders cor ON LOWER(TRIM(co.company_name)) = cor.company_name_lower
