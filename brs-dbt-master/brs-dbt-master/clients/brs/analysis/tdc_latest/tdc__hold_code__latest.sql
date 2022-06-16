{{
    config(
        materialized='incremental',
        unique_key='hold_code_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY hold_code_id ORDER BY last_updated_ts DESC) AS row_num
          FROM {{ ref('tdc__hold_code__base') }}
          {% if is_incremental() %}
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1