{{
config(
    materialized='table'
)
}}

{%- set start_date = "DATE('2021-01-11')"-%}

SELECT  snapshot_date
FROM    UNNEST(
            GENERATE_DATE_ARRAY(
                {{ start_date }}
                , CURRENT_DATE()
                , INTERVAL 1 DAY) 
            ) AS snapshot_date
