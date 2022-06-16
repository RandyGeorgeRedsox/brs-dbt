{{
    config(
        materialized= 'ephemeral'
    )
}}
SELECT  guid
      , email
      , usage_date
      , pages
      , identity_point_id
      , page_views_sum
      , okta_id
      , batch_date
      , team_nickname
      , team_id

FROM    {{ source('wheelhouse_red_sox', 'ballpark_views') }}