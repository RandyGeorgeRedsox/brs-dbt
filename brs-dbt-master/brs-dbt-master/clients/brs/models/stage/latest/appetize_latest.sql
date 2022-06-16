{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY TIMESTAMP(date_modified) DESC) AS row_num
          FROM    {{ source('stage', 'appetize_orders') }}
          {% if is_incremental() %}
              WHERE TIMESTAMP(date_modified) > (SELECT MAX(TIMESTAMP(date_modified)) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1