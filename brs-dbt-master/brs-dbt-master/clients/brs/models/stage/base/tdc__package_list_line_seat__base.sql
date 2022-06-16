select  package_list_line_seat_id as package_list_line_seat_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
    --   , aggregate_price
      , current_transaction_type_code as current_transaction_type_code
    --   , event_lower_price
    --   , event_upper_price
    --   , offer_id
    --   , package_seat_origin_code
    --   , should_be_rolled_over
    --   , reference_buyer_type_id
    --   , coupon_id
      , order_id as order_id
      , package_line_id as package_line_id
    --   , package_list_id
      , package_list_line_id as package_list_line_id
      , reference_price_scale_id as reference_price_scale_id
      , reference_seat_id as reference_seat_id
    --   , package_id
    --   , transaction_id
    --   , registered_patron_account_id
    --   , etl_batch_id

from    {{ source('wheelhouse_red_sox_ticketing', 'package_list_line_seat') }}



        