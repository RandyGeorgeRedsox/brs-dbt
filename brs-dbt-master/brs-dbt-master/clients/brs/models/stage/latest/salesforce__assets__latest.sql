{{
    config(
        materialized='incremental',
        unique_key='flywheel_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY flywheel_id ORDER BY imported_at_ts DESC) AS row_num
          FROM {{ ref('salesforce__assets__base') }}
          {% if is_incremental() %}
              WHERE imported_at_ts > (SELECT MAX(imported_at_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1