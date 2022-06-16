{{
    config(
        materialized="table",
        cluster_by=['email']
    )
}}

SELECT *
FROM {{ ref('wheelhouse_contact') }}