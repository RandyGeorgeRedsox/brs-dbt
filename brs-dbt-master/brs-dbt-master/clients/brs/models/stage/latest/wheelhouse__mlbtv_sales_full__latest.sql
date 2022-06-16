
{{
    config(
        materialized='incremental',
        unique_key='primary_key'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY mlbam_dw_row_modified_date DESC) AS row_num
          FROM {{ ref('wheelhouse__mlbtv_sales_full__base') }}
          {% if is_incremental() %}
              WHERE mlbam_dw_row_modified_date > (SELECT MAX(mlbam_dw_row_modified_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1