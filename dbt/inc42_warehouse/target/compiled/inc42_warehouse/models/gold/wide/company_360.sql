-- Gold company_360: ONE ROW = EVERYTHING about a company

WITH company AS (
    SELECT * FROM `bigquery-296406`.`silver_gold`.`dim_company`
),

inc42_contacts AS (
    SELECT
        company_name,
        COUNT(*) AS contacts_in_warehouse,
        SUM(CASE WHEN plus_status = 'active' THEN 1 ELSE 0 END) AS plus_members,
        SUM(ltv) AS total_ltv_from_company
    FROM `bigquery-296406`.`silver_silver`.`contacts`
    GROUP BY company_name
)

SELECT
    co.company_key,
    co.company_id,
    co.company_name,
    co.legal_name,
    co.domain,
    co.sector,
    co.sub_sector,
    co.business_model,
    co.company_status,
    co.founded_year,
    co.hq_city,
    co.hq_state,
    co.employee_count,
    co.latest_revenue_inr,
    co.latest_pat_inr,
    co.is_profitable,
    co.net_margin_pct,
    co.total_funding_inr,
    co.last_funding_stage,
    co.last_funding_date,
    co.key_investors,
    co.monthly_web_visits,
    co.app_rating,
    co.glassdoor_rating,

    -- Inc42 engagement
    COALESCE(ic.contacts_in_warehouse, 0) AS contacts_in_warehouse,
    COALESCE(ic.plus_members, 0) AS plus_members,
    COALESCE(ic.total_ltv_from_company, 0) AS total_ltv_from_company,

    CURRENT_TIMESTAMP() AS updated_at

FROM company co
LEFT JOIN inc42_contacts ic ON LOWER(TRIM(co.company_name)) = LOWER(TRIM(ic.company_name))