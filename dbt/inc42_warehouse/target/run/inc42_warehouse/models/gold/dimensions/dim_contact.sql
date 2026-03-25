
  
    

    create or replace table `bigquery-296406`.`silver_gold`.`dim_contact`
      
    
    

    
    OPTIONS()
    as (
      -- Gold dim_contact: master contact dimension with company denormalized IN (star schema)

SELECT
    ROW_NUMBER() OVER (ORDER BY c.unified_contact_id) AS contact_key,
    c.unified_contact_id,
    c.email,
    c.first_name,
    c.last_name,
    c.phone,
    c.company_name,
    -- Company fields DENORMALIZED into contact (star schema, not snowflake)
    co.sector AS company_sector,
    co.sub_sector AS company_sub_sector,
    co.business_model AS company_business_model,
    co.founded_year AS company_founded_year,
    co.employee_count AS company_employees,
    co.latest_revenue_inr AS company_revenue,
    co.total_funding_inr AS company_funding,
    co.last_funding_stage AS company_stage,
    co.is_profitable AS company_is_profitable,
    -- Professional
    c.designation,
    c.seniority,
    c.linkedin_url,
    c.city,
    c.state,
    c.country,
    c.user_type,
    -- Plus
    c.plus_membership_type,
    c.plus_status,
    c.plus_days_to_expiry,
    -- Engagement
    c.engagement_status,
    c.ltv,
    c.sessions,
    -- Lead
    c.lifecycle_stage,
    c.lead_status,
    c.hubspot_score,
    c.source_count

FROM `bigquery-296406`.`silver_silver`.`contacts` c
LEFT JOIN `bigquery-296406`.`silver_silver`.`companies` co
    ON LOWER(TRIM(c.company_name)) = LOWER(TRIM(co.company_name))
    );
  