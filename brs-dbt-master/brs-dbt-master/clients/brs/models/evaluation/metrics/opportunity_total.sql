{{
    config(
        materialized="table"
    )
}}
SELECT * EXCEPT(row_num)
FROM (
    SELECT  user_id
          , audience_id
          , snapshot_date
          , is_opportunity AS value
          , ROW_NUMBER() OVER(PARTITION BY user_id, audience_id, snapshot_date ORDER BY is_opportunity DESC) AS row_num
    
    FROM    {{ ref('opportunity') }}
)
WHERE row_num = 1