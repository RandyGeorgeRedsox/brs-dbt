{{
    config(
        materialized='incremental',
        unique_key = 'delivery_method_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY delivery_method_id ORDER BY last_updated_ts DESC) AS row_num
          FROM {{ ref('tdc__delivery_method__base') }}
          {% if is_incremental() %}
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1