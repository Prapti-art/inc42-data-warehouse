
  
    

    create or replace table `bigquery-296406`.`silver_gold`.`fact_marketing_touchpoints`
      
    
    

    
    OPTIONS()
    as (
      -- Gold fact_marketing_touchpoints: one row per email event per contact

WITH events AS (
    SELECT
        e.userId,
        e.event,
        JSON_VALUE(e.properties, '$.campaign_name') AS campaign_name,
        JSON_VALUE(e.properties, '$.channel') AS channel,
        e.timestamp AS event_timestamp
    FROM `bigquery-296406`.`bronze`.`customerio_events` e
),

contact_map AS (
    SELECT source_id, unified_contact_id
    FROM `bigquery-296406`.`silver`.`contact_source_xref`
    WHERE source_system = 'customerio'
)

SELECT
    ROW_NUMBER() OVER (ORDER BY ev.event_timestamp) AS touchpoint_id,
    dc.contact_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(ev.event_timestamp)) AS INT64) AS activity_date_key,
    ev.campaign_name,
    ev.channel,
    ev.event AS event_type,

    -- Measures (each 1 or 0, SUM across contacts for totals)
    CASE WHEN ev.event = 'Email Opened' THEN 1 ELSE 0 END AS opened,
    CASE WHEN ev.event = 'Email Clicked' THEN 1 ELSE 0 END AS clicked,
    CASE WHEN ev.event = 'Email Unsubscribed' THEN 1 ELSE 0 END AS unsubscribed,
    CASE WHEN ev.event = 'Push Sent' THEN 1 ELSE 0 END AS push_sent

FROM events ev
LEFT JOIN contact_map cm ON ev.userId = cm.source_id
LEFT JOIN `bigquery-296406`.`silver_gold`.`dim_contact` dc ON cm.unified_contact_id = dc.unified_contact_id
    );
  