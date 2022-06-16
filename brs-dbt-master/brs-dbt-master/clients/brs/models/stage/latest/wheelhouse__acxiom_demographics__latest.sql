{{
    config(
        materialized='incremental',
        unique_key='email'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY email ORDER BY last_acxiom_update_date DESC) AS row_num
          FROM {{ ref('wheelhouse__acxiom_demographics__base') }}
          {% if is_incremental() %}
              WHERE last_acxiom_update_date > (SELECT MAX(last_acxiom_update_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1