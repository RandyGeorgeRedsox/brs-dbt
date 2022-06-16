{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) as row_num
          FROM    {{ ref('wheelhouse__zetahub_events__base') }}
          {% if is_incremental() %}
              WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
          {% endif %}
          )
WHERE   row_num = 1