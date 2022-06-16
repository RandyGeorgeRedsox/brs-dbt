select  price_scale_id as price_scale_id
--       , created_by_operator_id
      , created_date as created_ts
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , price_scale_code as price_scale_code
      , description as description 
--       , display_order
--       , active
      , public_description as public_description
--       , color
--       , sng_tixx_max_offer_tixx_limit
--       , sng_tixx_min_offer_tixx_limit
--       , text_color
--       , price_scale_group_id
--       , report_price_scale_group_id
      -- , venue_id
--       , percentage_limit_type_code
--       , capacity_percentage_limit
--       , public_synopsis
--       , short_public_description
--       , ticket_quota
--       , membership_program_id
--       , membership_level_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

      

from    {{ source('wheelhouse_red_sox_ticketing', 'price_scale') }}