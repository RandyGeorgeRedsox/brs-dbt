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
            , MAX(frequency_metric) AS frequency_max
            , APPROX_QUANTILES(frequency_metric, 100) AS frequency_percentiles
           
    FROM    data
), 

ranges AS (
    SELECT    SQRT(observation_count) AS bin_count_sqrt
            , (1 + LOG(observation_count, 2)) AS bin_count_sturges

            , frequency_percentiles[offset(0)] AS frequency_bottom
            , frequency_percentiles[offset(99)] AS frequency_top
            , frequency_percentiles[offset(75)] - frequency_percentiles[offset(25)] AS frequency_iqr

    FROM      min_and_max
),

generate_frequency_buckets AS (
    SELECT    x AS lower_bound
            , IFNULL(LEAD(x) OVER(ORDER BY x), 1 + (SELECT ROUND(frequency_max) FROM min_and_max)) AS upper_bound 
    FROM      UNNEST(generate_array(
                  (SELECT 0 FROM ranges)
                , (SELECT 20 FROM ranges)
                , (SELECT 1 FROM ranges) )) AS x
)

SELECT  lower_bound
      , upper_bound 
      , COUNT(*) AS count
FROM    generate_frequency_buckets 
        JOIN data
            ON data.frequency_metric >= lower_bound AND data.frequency_metric < upper_bound 
GROUP BY 1, 2 

