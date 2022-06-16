
{{
    config(
        materialized="table"
    )
}}
WITH date_spine AS (
  SELECT
    snapshot_date
  FROM {{ ref('0_evaluation_date_spine') }}
)


, opportunity_cte AS (
  SELECT  
      salesforce_opportunity_id
    , provenue_account_id
    , status
    , amount
    , DATE(created_ts) AS created_date 
    , DATE(close_date) AS close_date
    , campaign_id
    , primary_opportunity_email
  FROM {{ ref('salesforce__opportunities__latest') }}
  WHERE DATE(created_ts) >= ( SELECT MIN(date_entered_campaign) FROM {{ ref('1_evaluation_ids') }} )
)

SELECT
    a.user_id
  , a.audience_id
  , d.snapshot_date
  , IF(b.created_date <= d.snapshot_date, 1, 0) AS is_opportunity
  , IF(b.close_date <= d.snapshot_date AND status = 'Closed Won', 1, 0) AS closed_won
  , COALESCE(SUM(IF(b.close_date <= d.snapshot_date AND status = 'Closed Won', amount, 0)), 0) AS closed_won_value
FROM {{ ref('1_evaluation_ids') }} AS a
INNER JOIN date_spine as d
  ON a.date_entered_campaign <= d.snapshot_date
LEFT OUTER JOIN opportunity_cte AS b
  ON a.salesforce_audience_export_campaign_id = b.campaign_id AND salesforce_audience_export_email = b.primary_opportunity_email
WHERE a.date_entered_campaign <= DATE(b.created_date)
GROUP BY   
    a.user_id
  , a.audience_id
  , d.snapshot_date
  , created_date
  , status
  , close_date