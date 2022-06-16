{{
    config(
        materialized="table"
    )
}}
SELECT  user_id
      , audience_id
      , snapshot_date
      , SUM(closed_won_value) AS value
 
FROM    {{ ref('opportunity') }}
GROUP BY user_id
       , audience_id
       , snapshot_date