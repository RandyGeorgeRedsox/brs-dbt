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
  , usage_date AS created_dt
  , COUNT(pages) AS value
  , CONCAT(email, '_', usage_date) AS row_key
FROM {{ ref('wheelhouse__ballpark_views__latest') }}

-- {% if is_incremental() %}
-- WHERE   usage_date >= (SELECT MAX(usage_date) FROM {{ this }} )
-- {% endif %}

GROUP BY email
  , usage_date
  , CONCAT(email, '_', usage_date)
