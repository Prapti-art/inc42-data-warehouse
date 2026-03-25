
  
    

    create or replace table `bigquery-296406`.`silver_gold`.`dim_date`
      
    
    

    
    OPTIONS()
    as (
      -- Gold dim_date: standard date dimension 2020-2030

WITH date_spine AS (
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(DAY FROM date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    FORMAT_DATE('%A', date) AS day_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    -- Indian fiscal year (starts April)
    CASE WHEN EXTRACT(MONTH FROM date) >= 4
        THEN EXTRACT(YEAR FROM date)
        ELSE EXTRACT(YEAR FROM date) - 1
    END AS fiscal_year,
    CONCAT('Q', CAST(CEILING(MOD(EXTRACT(MONTH FROM date) + 8, 12) + 1) / 3 AS INT64)) AS fiscal_quarter

FROM date_spine
    );
  