{{
    config(
        materialized='incremental'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY email, pages, page_views_sum, usage_date ORDER BY batch_date DESC) AS row_num
          FROM {{ ref('wheelhouse__ballpark_views__base') }}
          {% if is_incremental() %}
              WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1