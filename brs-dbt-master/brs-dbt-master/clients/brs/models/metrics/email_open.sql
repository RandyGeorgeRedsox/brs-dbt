{{
config(
    materialized= 'table',
    unique_key = 'row_key',
    partition_by={
      "field": "created_dt",
      "data_type": "date"
    },
    cluster_by = ['user_id', 'created_dt']
)
}}

SELECT  email AS user_id
      , DATE(event_time) AS created_dt
      , COUNT(DISTINCT event_time ) AS value
      , CONCAT(email, '_', DATE(event_time)) AS row_key
FROM {{ ref('wheelhouse__zetahub_events__latest') }} 
WHERE event_type = 'campaign_opened'

-- {% if is_incremental() %}
--   AND DATE(event_date) >= (SELECT MAX(DATE(event_date)) FROM {{ this }} )
-- {% endif %}

GROUP BY  email 
        , DATE(event_time)
        , CONCAT(email, '_', DATE(event_time))
