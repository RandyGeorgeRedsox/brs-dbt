{{
    config(
        materialized='table',
        unique_key='salesforce_opportunity_product_id'
    )
}}

WITH deleted_records AS (
    SELECT *
    FROM {{ ref('salesforce__opportunity_products__intermediate') }}
    WHERE salesforce_opportunity_product_id NOT IN (
    SELECT DISTINCT Id
    FROM {{ source('stage', 'salesforce__brs__opportunity_products') }} 
    WHERE imported_at_ts = (SELECT MAX(imported_at_ts) FROM {{ source('stage', 'salesforce__brs__opportunity_products') }})
    )
    AND is_deleted IS FALSE
)


SELECT *
FROM (
SELECT o.*
FROM {{ ref('salesforce__opportunity_products__intermediate') }} AS o
     LEFT OUTER JOIN deleted_records AS d
        ON o.salesforce_opportunity_product_id = d.salesforce_opportunity_product_id
WHERE d.salesforce_opportunity_product_id IS NULL
)
WHERE is_deleted IS NOT TRUE

