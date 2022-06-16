{{
    config(
        materialized='incremental',
        cluster_by=['form_id'],
        enabled=false
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY form_id, email, additional_comments, mobile_phone, DATETIME_TRUNC(created_date, MINUTE) ORDER BY created_date DESC) as row_num
          FROM    {{ ref('wheelhouse__forms2_detail__base') }}
          {% if is_incremental() %}
              WHERE created_date > (SELECT MAX(created_date) FROM {{ this }})
          {% endif %}
          
          )
WHERE   row_num = 1
