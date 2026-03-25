-- Silver companies: Datalabs as backbone, enriched with HubSpot/Tally data

SELECT
    dl.company_uuid AS company_id,
    dl.name AS company_name,
    dl.legal_name,
    dl.website,
    REPLACE(REPLACE(dl.website, 'https://', ''), 'http://', '') AS domain,
    dl.founded_year,
    dl.hq_city,
    dl.hq_state,
    dl.sector,
    dl.sub_sector,
    dl.business_model,
    dl.company_status,
    dl.employee_count,
    dl.latest_revenue_inr,
    dl.latest_pat_inr,
    CASE WHEN dl.latest_pat_inr > 0 THEN TRUE ELSE FALSE END AS is_profitable,
    SAFE_DIVIDE(dl.latest_pat_inr, dl.latest_revenue_inr) * 100 AS net_margin_pct,
    dl.total_funding_inr,
    dl.last_funding_stage,
    dl.last_funding_date,
    dl.key_investors,
    dl.monthly_web_visits,
    dl.app_rating,
    dl.glassdoor_rating,
    CURRENT_TIMESTAMP() AS updated_at

FROM {{ source('bronze', 'dl_company_table') }} dl
