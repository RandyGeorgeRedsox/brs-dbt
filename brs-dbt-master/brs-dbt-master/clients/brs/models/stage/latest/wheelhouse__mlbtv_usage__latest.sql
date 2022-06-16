{{
    config(
        materialized='incremental',
        unique_key='primary_key'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY mlbtv_usage_timestamp DESC) AS row_num
          FROM {{ ref('wheelhouse__mlbtv_usage__base') }}
          {% if is_incremental() %}
              WHERE mlbtv_usage_timestamp > (SELECT MAX(mlbtv_usage_timestamp) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1