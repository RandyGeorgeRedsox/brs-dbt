{{
    config(
        materialized='incremental',
        unique_key='package_id'
    )
}}

SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY package_id ORDER BY modified_date DESC) AS row_num
          FROM {{ ref('wheelhouse__ticket_details_packages_tdc__base') }}
          {% if is_incremental() %}
              WHERE modified_date > (SELECT MAX(modified_date) FROM {{ this }})
          {% endif %}
)
WHERE   row_num = 1