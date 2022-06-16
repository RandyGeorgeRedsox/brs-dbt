select  package_line_id as package_line_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
    --   , offer_id
    --   , package_change_status_code
      , package_order_origin_code as package_order_origin_code 
    --   , package_order_status_code
    --   , proration_factor
    --   , reservation_expiration_date
      , order_id as order_id
      , package_id as package_id
      , transaction_id as transaction_id
    --   , number_of_resv_package_seats
    --   , number_of_sold_package_seats
    --   , auto_renewal
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'package_line') }}