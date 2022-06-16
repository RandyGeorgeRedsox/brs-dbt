{{
  config(
    materialized="incremental",
    unique_key="eval_id",
    cluster_by=["audience_id"]
  )
}}

WITH audience_snapshot AS (
  SELECT  snap.audience_id
        , snap.snapshot_date
        , snap.audience_name
        , IF(audience_id IN (111, 112), ms.email, salesforce_audience_export_email) AS salesforce_audience_export_email
        , snap.salesforce_audience_export_campaign_name
        , snap.salesforce_audience_export_campaign_id
        , snap.salesforce_audience_export_last_name
        , snap.salesforce_audience_export_first_name
        , snap._composite_key
        , snap.pv_account_id
        , snap.country
        , snap.first_name
        , snap.last_name
        , snap.postal_code
        , snap.email
        , snap.is_control
        , snap.user_id

FROM    {{ ref('user_audience_map_snapshot__base') }} AS snap
LEFT JOIN {{ ref('master_store_contact') }} as ms
    ON snap.pv_account_id = ms.pv_account_id 
)


    , user_audiences AS (
  SELECT  
      u.pv_account_id AS user_id 
    , snap.audience_id
    , snap.is_control
    , snap.snapshot_date
    , snap.salesforce_audience_export_email
    , snap.salesforce_audience_export_campaign_id
    , ROW_NUMBER() OVER (PARTITION BY u.pv_account_id, snap.audience_id ORDER BY snap.snapshot_date ASC) AS row_num
  FROM audience_snapshot as snap
  LEFT JOIN {{ ref('master_store_contact') }} as u
    ON snap.salesforce_audience_export_email = u.email 
    -- ON snap.client_customer_id = c.client_customer_id
)

SELECT  
    user_id
  , audience_id
  , is_control
  , CONCAT(user_id, '_', audience_id) AS eval_id
  , snapshot_date AS date_entered_campaign
  , salesforce_audience_export_email
  , salesforce_audience_export_campaign_id
FROM user_audiences
WHERE row_num = 1
