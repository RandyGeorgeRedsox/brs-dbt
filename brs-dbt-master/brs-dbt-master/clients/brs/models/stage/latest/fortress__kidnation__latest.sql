{{
    config(
        materialized='incremental',
        unique_key='email'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY email ORDER BY date_modified DESC) AS row_num
          FROM {{ ref('fortress__kidnation__base') }}
          {% if is_incremental() %}
              WHERE date_modified > (SELECT MAX(date_modified) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1