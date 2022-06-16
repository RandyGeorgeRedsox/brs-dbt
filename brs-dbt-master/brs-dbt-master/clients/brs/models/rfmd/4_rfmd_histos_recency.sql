{{
    config(
        materialized="table",
    )
}}

WITH data AS (
    SELECT  * 
    FROM    {{ ref('1_rfmd_metrics')}}
), 

min_and_max AS (
    SELECT    COUNT(*) AS observation_count
            , MAX(recency_metric) AS recency_max  
            , APPROX_QUANTILES(recency_metric, 100) AS recency_percentiles             
    FROM    data
), 

ranges AS (
    SELECT    SQRT(observation_count) AS bin_count_sqrt
            , (1 + LOG(observation_count, 2)) AS bin_count_sturges

            , recency_percentiles[offset(0)] AS recency_bottom
            , recency_percentiles[offset(97)] AS recency_top
            , recency_percentiles[offset(75)] - recency_percentiles[offset(25)] AS recency_iqr
    FROM      min_and_max
),

generate_recency_buckets AS (
    SELECT    x AS lower_bound
            , IFNULL(LEAD(x) OVER(ORDER BY x), 1 + (SELECT ROUND(recency_max) FROM min_and_max)) AS upper_bound 
    FROM      UNNEST(generate_array(
                  (SELECT 0 FROM ranges)
                , (SELECT recency_top FROM ranges)
                , (SELECT ROUND((recency_top - recency_bottom) / 100) FROM ranges) )) AS x
)

SELECT  lower_bound
      , upper_bound 
      , COUNT(*) AS count
FROM    generate_recency_buckets 
        JOIN data
            ON data.recency_metric >= lower_bound AND data.recency_metric < upper_bound 
GROUP BY 1, 2 

