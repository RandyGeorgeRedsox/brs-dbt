{{
    config(
        materialized='incremental',
        unique_key='qualtrics_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY qualtrics_id ORDER BY response_date DESC) AS row_num
          FROM {{ ref('wheelhouse__qualtrics_voc_post_attendance_2022_full__base') }}
          {% if is_incremental() %}
              WHERE response_date > (SELECT MAX(response_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1