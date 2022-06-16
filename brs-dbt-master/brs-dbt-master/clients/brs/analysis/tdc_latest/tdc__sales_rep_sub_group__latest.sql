{{
    config(
        materialized='incremental',
        unique_key='sales_rep_sub_group_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY sales_rep_sub_group_id ORDER BY last_updated_ts DESC) AS row_num
          FROM {{ ref('tdc__sales_rep_sub_group__base') }}
          {% if is_incremental() %}
              WHERE last_updated_ts > (SELECT MAX(last_updated_ts) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1