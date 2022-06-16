{{
config(
    unique_key = 'row_key',
    partition_by={
      "field": "created_dt",
      "data_type": "date"
    },
    cluster_by = ['user_id', 'created_dt'],
)
}}

SELECT
  email AS user_id
  , DATE(mlbtv_usage_timestamp) AS created_dt
  , COUNT(DISTINCT mlbtv_usage_timestamp) AS value
  , CONCAT(email, '_', DATE(mlbtv_usage_timestamp)) AS row_key
FROM {{ ref('wheelhouse__mlbtv_usage__latest') }}

-- {% if is_incremental() %}
--   WHERE DATE(mlbtv_usage_timestamp) >= (SELECT MAX(DATE(mlbtv_usage_timestamp)) FROM {{ this }} )
-- {% endif %}

GROUP BY email
  , DATE(mlbtv_usage_timestamp)
  , CONCAT(email, '_', DATE(mlbtv_usage_timestamp))
