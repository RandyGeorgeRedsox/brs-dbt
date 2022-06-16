{{
    config(
        materialized='incremental',
        unique_key='pk'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY pk ORDER BY recorded_date DESC) AS row_num
          FROM {{ ref('wheelhouse__qualtrics_responses_full__base') }}
          {% if is_incremental() %}
              WHERE recorded_date > (SELECT MAX(recorded_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1