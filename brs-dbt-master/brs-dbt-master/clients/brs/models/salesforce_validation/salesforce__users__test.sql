{{
    config(
        materialized='table'
    )
}}

SELECT CAST(COUNT(DISTINCT owner_id) AS STRING) AS cnt
FROM  {{ ref('salesforce__opportunities__latest') }}
WHERE owner_id NOT IN (
    SELECT DISTINCT salesforce_user_id
    FROM {{ ref('salesforce__users__latest') }}
)
