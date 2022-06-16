{{
    config(
        materialized="table",
        cluster_by=['metric']
    )
}}

SELECT  *
      , 'recency' AS metric
FROM    {{ ref('4_rfmd_histos_recency')}}

UNION ALL

SELECT  *
      , 'frequency' AS metric
FROM    {{ ref('4_rfmd_histos_frequency')}}

UNION ALL

SELECT  *
      , 'monetary' AS metric
FROM    {{ ref('4_rfmd_histos_monetary')}}

UNION ALL

SELECT  *
      , 'age' AS metric
FROM    {{ ref('4_rfmd_histos_age')}}

