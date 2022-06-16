select  ticket_delivery_status_code as ticket_delivery_status_code
      , ticket_delivery_status_desc
      , created_date
      , last_updated_date as last_updated_ts
      -- , etl_batch_id

from {{ source('wheelhouse_red_sox_ticketing', 'ticket_delivery_status')}}