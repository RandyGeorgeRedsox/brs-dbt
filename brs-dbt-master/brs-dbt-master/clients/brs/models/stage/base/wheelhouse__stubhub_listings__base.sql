SELECT  snapshot_date
      , listing_id
      , CONCAT(listing_id, '_', listing_last_updated_date) AS _composite_key
      , listing_created_date
      , listing_last_updated_date
      , seller_id
    --   , seller_first_name
    --   , seller_last_name
    --   , seller_address1
    --   , seller_address2
    --   , seller_city_name
    --   , seller_state
    --   , seller_zipcode
    --   , seller_country
    --   , seller_phone1
    --   , seller_phone2
    --   , seller_email
      , event_id
      , seat_zone
      , seat_section
      , row_desc
      , seats
      , tickets_remaining
      , current_price
    --   , dw_add_tsp
      , event_descr
    --   , home_team
    --   , away_team
    --   , sh_event_type
      , event_date
    --   , event_cancelled_yn
    --   , batch_date
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'stubhub_listings') }} 
