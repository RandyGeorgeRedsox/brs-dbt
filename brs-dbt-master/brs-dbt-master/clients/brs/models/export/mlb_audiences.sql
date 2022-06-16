{{
config(
    partition_by={
      "field": "snapshot_ts",
      "data_type": "timestamp"
    },
    cluster_by=['audience_id']
)
}}

WITH cte AS (
SELECT  DISTINCT REPLACE(REPLACE(REPLACE(SUBSTR(LOWER(salesforce_audience_export_campaign_name), LENGTH(REGEXP_EXTRACT(LOWER(salesforce_audience_export_campaign_name), r'^(.*)|.*'))-1), 'fb', 'facebook'), 'adwords', 'google_ads'), 'zeta', 'zeta') AS destination
      , salesforce_audience_export_campaign_id
FROM    {{ ref('user_audience_map_snapshot__base') }} 
-- WHERE LOWER(salesforce_audience_export_campaign_name) like '%bigquery%'
-- AND  (LOWER(salesforce_audience_export_campaign_name) like '%fb%'
-- OR   LOWER(salesforce_audience_export_campaign_name) like '%adwords%'
-- OR   LOWER(salesforce_audience_export_campaign_name) like '%zeta%'
-- )
)
, provenue_salesforce_email AS (
   SELECT DISTINCT email 
   FROM (
      SELECT DISTINCT email
      FROM {{ ref('provenue_contact') }}
      UNION ALL
      SELECT DISTINCT email
      FROM {{ ref('salesforce_contact') }}
   )
)

SELECT  audience_id
      , IF(audience_id = 86, 'DEPRECATED April Single Game Buyers', salesforce_audience_export_campaign_name) AS audience_name
      , TIMESTAMP(snapshot_date) AS snapshot_ts
      , salesforce_audience_export_email AS email
      , salesforce_audience_export_last_name AS last_name
      , salesforce_audience_export_first_name AS first_name
      , NULL AS device_id
      , is_control AS control
      , _composite_key AS user_id
      , CASE WHEN audience_id IN (86, 87) THEN 'facebook'
             WHEN audience_id IN (90, 106) THEN 'zeta'
             ELSE destination
             END AS destination
      , CASE WHEN audience_id IN (90, 106) THEN 'marketing'
             ELSE CAST(NULL AS STRING)
             END AS mailing_type 
      , CASE WHEN audience_id = 90 THEN 14
             WHEN audience_id = 106 THEN 14
             ELSE CAST(NULL AS INT64)
             END AS mlb_scrub_query_id 
       , CASE WHEN audience_id = 106 THEN '410616'
             ELSE CAST(NULL AS STRING)
             END AS campaign_id
       , CASE WHEN audience_id = 106 THEN 'ongoing'
             ELSE CAST(NULL AS STRING)
             END AS campaign_type      
      , CAST(NULL AS STRING) AS dynamic_content 
      , IF(audience_id IN (86, 87, 90, 106), TRUE, NULL) AS legal_collection
      , IF(audience_id IN (86, 87, 90, 106), TRUE, NULL) AS legal_user_opt_in
      , IF(audience_id IN (86, 87, 90, 106), TRUE, NULL) AS legal_opt_in_vetted

FROM    {{ ref('user_audience_map_snapshot__base') }} a
        LEFT OUTER JOIN cte
            ON a.salesforce_audience_export_campaign_id = cte.salesforce_audience_export_campaign_id
        INNER JOIN provenue_salesforce_email AS ps
            ON a.salesforce_audience_export_email = ps.email
WHERE audience_id IN (86, 87, 90, 106)

UNION ALL

SELECT  audience_id
      , IF(audience_id = 86, 'DEPRECATED April Single Game Buyers', a.audience_name) AS audience_name
      , TIMESTAMP(snapshot_date) AS snapshot_ts
      , ms.email
      , ms.last_name
      , ms.first_name
      , NULL AS device_id
      , is_control AS control
      , a._composite_key AS user_id
      , CASE WHEN audience_id IN (86, 87, 111, 112) THEN 'facebook'
             WHEN audience_id IN (90, 106) THEN 'zeta'
             ELSE destination
             END AS destination
      , CASE WHEN audience_id IN (90, 106) THEN 'marketing'
             ELSE CAST(NULL AS STRING)
             END AS mailing_type 
      , CASE WHEN audience_id = 90 THEN 14
             WHEN audience_id = 106 THEN 14
             ELSE CAST(NULL AS INT64)
             END AS mlb_scrub_query_id 
       , NULL AS campaign_id
       , NULL AS campaign_type 
      , CAST(NULL AS STRING) AS dynamic_content 
      , IF(audience_id IN (111, 112), TRUE, NULL) AS legal_collection
      , IF(audience_id IN (111, 112), TRUE, NULL) AS legal_user_opt_in
      , IF(audience_id IN (111, 112), TRUE, NULL) AS legal_opt_in_vetted

FROM    {{ ref('user_audience_map_snapshot__base') }} a
        LEFT OUTER JOIN {{ ref('master_store_contact') }} AS ms
            ON a.pv_account_id = ms.pv_account_id
        LEFT OUTER JOIN cte
            ON a.salesforce_audience_export_campaign_id = cte.salesforce_audience_export_campaign_id
       --  INNER JOIN provenue_salesforce_email AS ps
       --      ON a.salesforce_audience_export_email = ps.email
WHERE audience_id IN (111, 112)
