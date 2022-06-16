{{
    config(
        materialized='incremental',
        unique_key='contact_id'
    )
}}
SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY last_modified_ts DESC, imported_at_ts DESC) AS row_num
          FROM {{ ref('salesforce__contacts__base') }}
          {% if is_incremental() %}
              WHERE last_modified_ts > (SELECT MAX(last_modified_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1