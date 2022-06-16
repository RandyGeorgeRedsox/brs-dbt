{{
    config(
        materialized='incremental',
        unique_key='pv_account_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY pv_account_id ORDER BY snapshot_date DESC) AS row_num
          FROM {{ source('audience_exports', 'snapshots_106_snapshots') }}
          {% if is_incremental() %}
              WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1


