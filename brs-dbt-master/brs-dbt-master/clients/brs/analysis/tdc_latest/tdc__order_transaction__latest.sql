{{
    config(
        materialized='incremental',
        unique_key='order_transaction_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY order_transaction_id ORDER BY created_ts DESC) AS row_num
          FROM {{ ref('tdc__order_transaction__base') }}
          {% if is_incremental() %}
              WHERE created_ts > (SELECT MAX(created_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1