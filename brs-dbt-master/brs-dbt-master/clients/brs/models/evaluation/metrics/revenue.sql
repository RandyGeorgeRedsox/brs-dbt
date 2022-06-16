--revenue for each user in table since campaign began
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

--need to create a transaction model to account for this cte: account_id, post_date (ticket/merch purchase date), ticket/merch spend (on that post date)
, transactions_cte AS (
  SELECT  
      pv_account_id AS user_id
    , DATE(created_date) AS purchase_date 
    , ticket_spending AS order_spending
  FROM {{ ref('master_store_transaction') }}
  WHERE ticket_spending > 0
    AND DATE(created_date) >= ( SELECT MIN(date_entered_campaign) FROM {{ ref('1_evaluation_ids') }} )
)

SELECT
    a.user_id
  , a.audience_id
  , d.snapshot_date
  , SUM(IF(b.purchase_date <= d.snapshot_date, b.order_spending, 0)) AS value
FROM {{ ref('1_evaluation_ids') }} AS a
INNER JOIN date_spine as d
  ON a.date_entered_campaign <= d.snapshot_date
LEFT OUTER JOIN transactions_cte AS b
  ON a.user_id = b.user_id
WHERE a.date_entered_campaign <= DATE(b.purchase_date)
GROUP BY   
    a.user_id
  , a.audience_id
  , d.snapshot_date
