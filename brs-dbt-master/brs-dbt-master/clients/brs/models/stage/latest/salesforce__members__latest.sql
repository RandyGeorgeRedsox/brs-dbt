{{
    config(
        materialized='incremental',
        unique_key='salesforce_member_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY salesforce_member_id ORDER BY imported_at_ts DESC) AS row_num
          FROM {{ ref('salesforce__members__base') }}
          {% if is_incremental() %}
              WHERE imported_at_ts > (SELECT MAX(imported_at_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1