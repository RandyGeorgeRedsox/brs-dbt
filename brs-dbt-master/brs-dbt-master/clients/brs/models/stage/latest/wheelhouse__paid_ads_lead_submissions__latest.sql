{{
    config(
        materialized='incremental',
        cluster_by=['submission_id']
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY submission_id ORDER BY form_submission_tsp DESC) as row_num
          FROM    {{ ref('wheelhouse__paid_ads_lead_submissions__base') }}
          {% if is_incremental() %}
              WHERE form_submission_tsp > (SELECT MAX(form_submission_tsp) FROM {{ this }})
          {% endif %}
          
          )
WHERE   row_num = 1
