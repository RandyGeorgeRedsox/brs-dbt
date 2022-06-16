{{
    config(
        materialized='incremental',
        unique_key='primary_key'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY order_date DESC) AS row_num
          FROM {{ ref('wheelhouse__shop_sales__base') }}
          {% if is_incremental() %}
              WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1