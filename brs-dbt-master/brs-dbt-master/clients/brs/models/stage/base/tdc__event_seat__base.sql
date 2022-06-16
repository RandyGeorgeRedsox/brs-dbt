select  event_id as event_id
      , seat_id as seat_id
      , concat(seat_id, '-', event_id) as event_seat_id
      , attending_patron_account_id as attending_patron_account_id
--       , buyer_type_id
--       , created_date
      , financial_patron_account_id as financial_patron_account_id
--       , hold_code_id
--       , is_available
      , last_updated_date as last_updated_ts
      , order_id as order_id
--       , order_line_item_id
      , price as price
      , cast(price_scale_id as int64) as price_scale
--       , remove_from_capacity
--       , reservation_code_id
      , section_id as section_id
      -- , sold_price_scale_id 
      , ticket_id as ticket_id
--       , transaction_id
--       , usage_event_id
--       , removed_from_venue_config
--       , config_hold_last_updated_date
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'event_seat') }}