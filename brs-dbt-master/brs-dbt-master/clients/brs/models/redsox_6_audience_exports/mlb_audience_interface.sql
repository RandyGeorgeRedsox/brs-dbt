{{
    config(
        materialized='table',
        partition_by={
      "field": "snapshot_ts",
      "data_type": "timestamp"
    }
    )
}}

SELECT
  audience_id
  , audience_name
  , snapshot_ts
  , ms.email
  , ms.last_name
  , ms.first_name
  , is_control AS control
  , user_id
  , destination
  , email_type AS mailing_type
  , COALESCE(CAST(a.email_list_id AS INT64), 14) AS mlb_scrub_query_id
  , campaign_id
  , campaign_type
  , organization AS org_id
  , dynamic_content
  , SAFE_CAST(device_id AS INT64) AS device_id
  , CAST(info_collected_via_mlb_privacy_policy AS BOOL) AS legal_collection
  , CAST(end_user_opt_in AS BOOL) AS legal_user_opt_in
  , CAST(opt_in_language_approved_vetted AS BOOL) AS legal_opt_in_vetted
FROM {{ ref('audience_snapshots') }} AS a
     LEFT OUTER JOIN {{ ref('master_store_wheelhouse_contact') }} AS ms
  ON a.email = ms.email
WHERE destination IN ('zeta', 'facebook_ads', 'google_ads')

