
{{
    config(
        materialized='incremental'
    )
}}

SELECT * EXCEPT(row_num)
FROM ( SELECT
  *
  , ROW_NUMBER() OVER (
    PARTITION BY email, audience_id, destination ORDER BY snapshot_ts DESC
  ) AS row_num
  FROM {{ ref('audience_snapshots_all') }}
  {% if is_incremental() %}
    WHERE snapshot_ts > (SELECT MAX(snapshot_ts) FROM {{ this }})
  {% endif %}
)
WHERE row_num = 1

