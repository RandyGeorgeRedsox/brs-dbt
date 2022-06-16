SELECT  data_source
    --   , team_id
    --   , advertiser_name
    --   , site_name
      , form_submission_tsp
      , form_submission_date
    --   , days_since_submission
    --   , hours_since_submission
      , submission_id
      , placement_id
      , placement_name
      , site_type
      , ad_type
      , initiative
      , budget
      , ad_exchange
      , promo
      , image
      , ad_size
      , ad_format
      , promo_bucket
      , geographic
      , demographic
    --   , psychographic
    --   , flight_start
    --   , flight_end
      , email
      , first_name
      , last_name
      , phone_number
      , select_the_time_that_works_best_for_you
      , state
      , street_address
      , zip_code
      , company
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'paid_ads_lead_submissions')}}
