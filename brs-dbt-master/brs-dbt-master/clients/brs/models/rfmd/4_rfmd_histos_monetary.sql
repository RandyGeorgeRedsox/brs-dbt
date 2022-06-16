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
            , MAX(monetary_metric) AS monetary_max
            , APPROX_QUANTILES(monetary_metric, 100) AS monetary_percentiles              
    FROM    data
), 

ranges AS (
    SELECT    SQRT(observation_count) AS bin_count_sqrt
            , (1 + LOG(observation_count, 2)) AS bin_count_sturges

            , monetary_percentiles[offset(0)] AS monetary_bottom
            , monetary_percentiles[offset(95)] AS monetary_top
            , monetary_percentiles[offset(75)] - monetary_percentiles[offset(25)] AS monetary_iqr
    FROM      min_and_max
),

generate_monetary_buckets AS (
    SELECT    x AS lower_bound
            , IFNULL(LEAD(x) OVER(ORDER BY x), 1 + (SELECT ROUND(monetary_max) FROM min_and_max)) AS upper_bound 
    FROM      UNNEST(generate_array(
                  (SELECT 0 FROM ranges)
                , (SELECT monetary_top FROM ranges)
                , (SELECT ROUND((monetary_top - monetary_bottom) / 30) FROM ranges) )) AS x
)

SELECT  lower_bound
      , upper_bound 
      , COUNT(*) AS count
FROM    generate_monetary_buckets 
        JOIN data
            ON data.monetary_metric >= lower_bound AND data.monetary_metric < upper_bound 
GROUP BY 1, 2 
