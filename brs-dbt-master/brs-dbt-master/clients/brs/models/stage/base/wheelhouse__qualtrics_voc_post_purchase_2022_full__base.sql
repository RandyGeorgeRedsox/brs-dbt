SELECT  qualtrics_id
      , DATE(response_date) AS response_date
      , vendor_ux_numrat
      , purchase_intent
      , team_interest
      , team_favorite
      , email
      , preason_rank_price
      , preason_question
      , preason_ot
      , preason_rank_teamperform
      , preason_rank_opponent
      , preason_rank_weather
      , preason_rank_vacation
      , preason_rank_celebration
      , preason_rank_giveaway
      , preason_rank_theme
      , preason_rank_friends_family
      , preason_rank_discount
      , preason_rank_advertisement
      , preason_rank_socialmedia
      , attend_num_plan
      

FROM    {{ source('wheelhouse_red_sox', 'qualtrics_voc_post_purchase_2022_full') }} 

