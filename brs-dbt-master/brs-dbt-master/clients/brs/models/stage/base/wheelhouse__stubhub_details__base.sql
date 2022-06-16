SELECT  event_type
      , event_date
    --   , event_name
      , away_team
      , confirmation_date
      , seat_section
      , seat_row
    --   , quantity
      , sale_amount
    --   , buyer_first_name
    --   , buyer_last_name
    --   , buyer_phone_1
    --   , buyer_phone_2
    --   , buyer_address_1
    --   , buyer_address_2
    --   , buyer_address_city
    --   , buyer_address_state
    --   , buyer_address_zip
      , buyer_email
    --   , seller_first_name
    --   , seller_last_name
    --   , seller_phone_1
    --   , seller_phone_2
    --   , seller_address_1
    --   , seller_address_2
    --   , seller_address_city
    --   , seller_address_state
    --   , seller_address_zip
      , seller_email
    --   , buyer_stubhub_id
    --   , seller_stubhub_id
      , seats
    --   , mlbam_email_id_buyer
    --   , mlbam_email_id_seller
      , batch_date
    --   , team_nickname
FROM    {{ source('wheelhouse_red_sox', 'stubhub_details') }} 
