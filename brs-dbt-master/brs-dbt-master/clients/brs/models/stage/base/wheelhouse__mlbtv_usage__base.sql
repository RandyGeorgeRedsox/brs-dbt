WITH cte_a AS (
SELECT  mlbtv_usage_id
      , CONCAT(mlbtv_usage_id, '_', IFNULL(email, '')) AS primary_key
      , requested_at
      , requested_at_date_pt
      , identity_point_id
      , user_id
      , email
      , product_type
      , media_id
      , media_title
      , home_team
      , away_team
      , game_pk
      , media_feed_type
      , free_game_of_the_day
      , same_day_stream
      , entitlements
      , session_id
      , device_type
      , device_category
      , country_code
      , zip_code
      , state_or_province
      , mlbtv_usage_timestamp
      , stream_type
      , batch_date
      , team_nickname
      , team_id
FROM    {{ source('wheelhouse_red_sox', 'mlbtv_usage') }} 
)
, cte_b AS (
SELECT c.* 
     , ROW_NUMBER() OVER(PARTITION BY primary_key ORDER BY mlbtv_usage_timestamp DESC) row_num
FROM cte_a AS c
)
SELECT * EXCEPT(row_num)
FROM cte_b
WHERE row_num = 1