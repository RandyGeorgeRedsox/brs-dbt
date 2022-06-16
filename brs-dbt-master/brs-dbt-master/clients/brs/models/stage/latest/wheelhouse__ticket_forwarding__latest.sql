{{
    config(
        materialized='incremental',
        unique_key='forward_reporting_activity_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY forward_reporting_activity_id ORDER BY batch_date DESC) AS row_num
          FROM {{ ref('wheelhouse__ticket_forwarding__base') }}
          {% if is_incremental() %}
              WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1