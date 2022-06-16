SELECT  qualtrics_id
      , response_date
      , vendor_ux_numrat
      , purchase_intent
      , team_interest
      , team_favorite
      , email

FROM    {{ source('wheelhouse_red_sox', 'qualtrics_voc_post_purchase_2021_full') }} 
