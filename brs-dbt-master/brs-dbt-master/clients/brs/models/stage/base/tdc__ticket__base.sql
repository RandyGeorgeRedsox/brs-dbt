select  ticket_id as ticket_id
--       , created_by_operator_id  
      , created_date as created_ts
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
--       , expanded_status_code
--       , note_text
      , payment_status_code as status
      , price as price
--       , seating_area_type_code
      , buyer_type_id as buyer_type_id
--       , coupon_id
--       , current_bulk_print_job_id
--       , current_sold_market_offer_id
      , hold_code_id as hold_code_id
--       , note_type_id
--       , open_item_liability_id
      , order_id as order_id
      , order_line_item_id as order_line_item_id
      , price_scale_id as price_scale_id
      , remove_order_line_item_id as remove_order_line_item_id
      , seat_id as seat_id
      , transaction_id as transaction_id
--       , inclusive_tax_id
--       , inclusive_tax_rate
--       , inclusive_tax_amount
--       , original_ticket_id
--       , seat_assignment_transaction_id
--       , promotion_id
--       , registered_patron_account_id
--       , trig_by_sales_group_id
--       , disc_by_sales_group_id
--       , fully_insured_price
--       , insurance_premium_amount
--       , insurance_policy_number
--       , print_restricted_esd_id
--       , offer_code_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts



from    {{ source('wheelhouse_red_sox_ticketing', 'ticket') }}