{{
    config(
        cluster_by=['email_id']
    )
}}

SELECT * EXCEPT (row_num)
FROM (
    SELECT  email_id
          , contact_info
          , web_usage
          , email
          , ballpark
          , mlb_products
          , mlbtv_usage
          , mlb_audio
          , ticketing
          , shop
          , data_science
          , forms
          , summary
          , segments
          , team_nickname
          , ROW_NUMBER() OVER(PARTITION BY email_id ORDER BY contact_info.email DESC) AS row_num

    FROM    {{ source('wheelhouse_red_sox', 'one_view') }}
)
WHERE row_num = 1