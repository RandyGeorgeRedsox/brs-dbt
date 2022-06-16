{{
    config(
        materialized="table",
        unique_field="pv_account_id",
        cluster_by=["pv_account_id"]
    )
}}

WITH active_quartiles_cte AS (
    SELECT  * EXCEPT(calculation_date)
    FROM    {{ ref('2_rfmd_quartiles') }}
    WHERE   calculation_date = (SELECT MAX(calculation_date) FROM {{ ref('2_rfmd_quartiles') }})
)

SELECT  pv_account_id
      , CASE WHEN recency_metric <= recency_q25 THEN 1
             WHEN recency_metric <= recency_q50 THEN 2
             WHEN recency_metric <= recency_q75 THEN 3
             WHEN recency_metric > recency_q75 THEN 4
             ELSE NULL
      END AS recency
      , CASE WHEN frequency_metric <= frequency_q25 THEN 1
             WHEN frequency_metric <= frequency_q50 THEN 2
             WHEN frequency_metric <= frequency_q75 THEN 3
             WHEN frequency_metric > frequency_q75 THEN 4
             ELSE NULL
      END AS frequency
      , CASE WHEN monetary_metric <= monetary_q25 THEN 4
             WHEN monetary_metric <= monetary_q50 THEN 3
             WHEN monetary_metric <= monetary_q75 THEN 2
             WHEN monetary_metric > monetary_q75 THEN 1
             ELSE NULL
      END AS monetary
      , CASE WHEN age_metric <= age_q25 THEN 1
             WHEN age_metric <= age_q50 THEN 2
             WHEN age_metric <= age_q75 THEN 3
             WHEN age_metric > age_q75 THEN 4
             ELSE NULL
      END AS age

FROM    {{ ref('1_rfmd_metrics') }}
        CROSS JOIN active_quartiles_cte