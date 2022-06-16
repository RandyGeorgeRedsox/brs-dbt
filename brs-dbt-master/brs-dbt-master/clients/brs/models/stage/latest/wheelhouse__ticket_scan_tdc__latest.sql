{{
    config(
        materialized='incremental',
        unique_key='ticket_print_scan_id'
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY ticket_print_scan_id ORDER BY last_updated_date DESC) as row_num
          FROM    {{ ref('wheelhouse__ticket_scan_tdc__base') }}
          {% if is_incremental() %}
              WHERE last_updated_date > (SELECT MAX(last_updated_date) FROM {{ this }})
          {% endif %}
          )
WHERE   row_num = 1