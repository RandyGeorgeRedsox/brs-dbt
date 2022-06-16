{{
    config(
        materialized="table",
        unique_key= "mlbtv_usage_id",
        cluster_by=["mlbtv_usage_id"]
    )
}}

SELECT  mlbtv_usage_id
      , primary_key
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
FROM    {{ ref('wheelhouse__mlbtv_usage__base') }} 