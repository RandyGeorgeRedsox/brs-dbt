{{
    config(
        materialized='table'
    )
}}

SELECT CAST(COUNT(DISTINCT salesforce_opportunity_id) AS STRING) AS cnt
FROM  {{ ref('salesforce__opportunity_products__latest') }}
WHERE salesforce_opportunity_id NOT IN (
    SELECT DISTINCT salesforce_opportunity_id
    FROM {{ ref('salesforce__opportunities__latest') }}
)
AND is_deleted != TRUE

UNION ALL

SELECT CAST(COUNT(DISTINCT event_inventory) AS STRING) AS cnt
FROM  {{ ref('salesforce__opportunity_products__latest') }}
WHERE event_inventory NOT IN (
    SELECT DISTINCT salesforce_event_inventory_id
    FROM {{ ref('salesforce__event_inventories__latest') }}
)

UNION ALL

SELECT CAST(COUNT(DISTINCT product_id) AS STRING) AS cnt
FROM  {{ ref('salesforce__opportunity_products__latest') }}
WHERE product_id NOT IN (
    SELECT DISTINCT salesforce_products_id
    FROM {{ ref('salesforce__products__latest') }}
)

UNION ALL

SELECT CAST(COUNT(DISTINCT fenway_event) AS STRING) AS cnt
FROM  {{ ref('salesforce__opportunity_products__latest') }}
WHERE fenway_event NOT IN (
    SELECT DISTINCT salesforce_fenway_event_id
    FROM {{ ref('salesforce__fenway_events__latest') }}
)