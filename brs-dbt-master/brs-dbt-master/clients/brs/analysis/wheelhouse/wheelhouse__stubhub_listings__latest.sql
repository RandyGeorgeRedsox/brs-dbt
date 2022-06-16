{{
    config(
        materialized='incremental',
        unique_key='_composite_key'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY _composite_key ORDER BY snapshot_date DESC) AS row_num
          FROM {{ ref('wheelhouse__stubhub_listings__base') }}
          {% if is_incremental() %}
              WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1