select  package_list_line_id as package_list_line_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
    --   , bundle_list_qty_type_code
    --   , expanded_date
    --   , expanded_status_code
    --   , max_bundled_quantity
    --   , min_bundled_quantity
    --   , offer_id
    --   , same_buyer_types_seats
    --   , seating_status_code
      , order_id as order_id
      , package_line_id as package_line_id
    --   , package_list_id
    --   , reservation_code_id
      , package_id as package_id
      , transaction_id as transaction_id
    --   , number_of_resv_pkg_list_seats
    --   , number_of_sold_pkg_list_seats
    --   , etl_batch_id

from    {{ source('wheelhouse_red_sox_ticketing', 'package_list_line') }}

 
