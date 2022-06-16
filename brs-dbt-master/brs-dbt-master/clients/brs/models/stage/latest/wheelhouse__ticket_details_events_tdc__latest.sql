{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dw_modified_date DESC) as row_num
          FROM    {{ ref('wheelhouse__ticket_details_events_tdc__base') }}
          {% if is_incremental() %}
              WHERE dw_modified_date > (SELECT MAX(dw_modified_date) FROM {{ this }})
          {% endif %}
          )
WHERE   row_num = 1
