{{
    config(
        materialized="incremental",
        unique_key="calculation_date",
        cluster_by=["calculation_date"]
    )
}}

WITH quantiles_cte AS (
    SELECT  APPROX_QUANTILES(recency_metric, 100) AS recency_percentiles 
          , APPROX_QUANTILES(frequency_metric, 100) AS frequency_percentiles
          , APPROX_QUANTILES(monetary_metric, 100) AS monetary_percentiles
          , APPROX_QUANTILES(age_metric, 100) AS age_percentiles
    FROM {{ ref('1_rfmd_metrics') }}
)

SELECT  CURRENT_DATE AS calculation_date
      , recency_percentiles[offset(25)] AS recency_q25 
      , recency_percentiles[offset(50)] AS recency_q50
      , recency_percentiles[offset(75)] AS recency_q75 
      , frequency_percentiles[offset(25)] AS frequency_q25 
      , frequency_percentiles[offset(50)] AS frequency_q50
      , frequency_percentiles[offset(75)] AS frequency_q75 
      , monetary_percentiles[offset(25)] AS monetary_q25
      , monetary_percentiles[offset(50)] AS monetary_q50 
      , monetary_percentiles[offset(75)] AS monetary_q75
      , age_percentiles[offset(25)] AS age_q25
      , age_percentiles[offset(50)] AS age_q50 
      , age_percentiles[offset(75)] AS age_q75
FROM    quantiles_cte

{% if is_incremental() %}
WHERE   CURRENT_DATE > (SELECT MAX(calculation_date) FROM {{ this }})
{% endif %}
