select  service_charge_item_id as service_charge_item_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , actual_amount as actual_amount
    --   , adjustment_type_code
    --   , calculated_amount
    --   , edited_reason_note_text
    --   , market_type_code
    --   , payment_status_code
      , transaction_type_code as transaction_type_code
    --   , delivery_id
    --   , edited_reason_note_type_id
    --   , negating_service_chrg_item_id
      , order_id as order_id
    --   , order_line_item_id
    --   , package_line_id
      , service_charge_id as service_charge_id
      , ticket_id as ticket_id
    --   , ticket_delivery_id
    --   , transaction_id
    --   , inclusive_tax_id
    --   , inclusive_tax_rate
    --   , inclusive_tax_amount
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts
 

from    {{ source('wheelhouse_red_sox_ticketing', 'service_charge_item') }}