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
    SELECT  COUNT(*) AS observation_count
          , MAX(age_metric) AS age_max   
          , APPROX_QUANTILES(age_metric, 100) AS age_percentiles                 
    FROM    data
),

ranges AS (
    SELECT    SQRT(observation_count) AS bin_count_sqrt
            , (1 + LOG(observation_count, 2)) AS bin_count_sturges

            , age_percentiles[offset(0)] AS age_bottom
            , age_percentiles[offset(97)] AS age_top
            , age_percentiles[offset(75)] - age_percentiles[offset(25)] AS age_iqr
    FROM      min_and_max
),

generate_age_buckets AS (
    SELECT    x AS lower_bound
            , IFNULL(LEAD(x) OVER(ORDER BY x), 1 + (SELECT ROUND(age_max) FROM min_and_max)) AS upper_bound 
    FROM      UNNEST(generate_array(
                  (SELECT 0 FROM ranges)
                , (SELECT age_top FROM ranges)
                , (SELECT ROUND((age_top - age_bottom) / 100) FROM ranges) )) AS x
)

SELECT  lower_bound
      , upper_bound 
      , COUNT(*) AS count
FROM    generate_age_buckets 
        JOIN data
            ON data.age_metric >= lower_bound AND data.age_metric < upper_bound 
GROUP BY 1, 2 
