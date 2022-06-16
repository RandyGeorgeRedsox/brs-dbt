select  order_line_item_id as order_line_item_id
--       , created_by_operator_id
      , created_date as created_ts
--       , capacity_processed_type_code
--       , complete_exchange_eligible
--       , market_offer_type_code
      , market_type_code as market_type_code
--       , offer_id
--       , offer_item_id
--       , reservation_expiration_date
--       , sales_type_code
      , transaction_type_code as transaction_type_code
      , event_id as event_id
      , order_id as order_id
      , package_line_id as package_line_id
--       , package_list_id
      , package_list_line_id as package_list_line_id
--       , reservation_code_id
      , package_id as package_id
      , transaction_id as transaction_id
      , usage_event_id as usage_event_id
--       , offer_seat_selection_type_code
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'order_line_item') }}