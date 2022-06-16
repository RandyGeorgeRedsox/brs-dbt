select  ticket_delivery_id as ticket_delivery_id
      , created_by_operator_id
      , created_date as created_ts
      , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , ticket_delivery_status_code
      , delivery_id
      , ticket_id
      , delivery_forward_status_code
      , transaction_id
      , remove_transaction_id
      -- , etl_batch_id

from {{ source('wheelhouse_red_sox_ticketing', 'ticket_delivery')}}