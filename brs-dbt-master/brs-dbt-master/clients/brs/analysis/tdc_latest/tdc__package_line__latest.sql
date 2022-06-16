{{
    config(
        materialized='incremental',
        unique_key='package_line_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY package_line_id ORDER BY last_updated_ts DESC) AS row_num
          FROM {{ ref('tdc__package_line__base') }}
          {% if is_incremental() %}
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1