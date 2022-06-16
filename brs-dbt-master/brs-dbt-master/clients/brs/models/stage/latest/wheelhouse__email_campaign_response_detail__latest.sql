{{
    config(
        materialized='incremental',
        unique_key='primary_key'
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY batch_date DESC) AS row_num
          FROM {{ ref('wheelhouse__email_campaign_response_detail__base') }}
          {% if is_incremental() %}
              WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1
