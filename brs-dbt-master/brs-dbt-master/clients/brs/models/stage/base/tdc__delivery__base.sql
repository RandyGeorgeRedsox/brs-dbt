select  delivery_id as delivery_id
--       , created_by_operator_id
      , created_date as created_ts
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
--       , addr1
--       , addr2
--       , city
--       , country_code
--       , postal_code
--       , sub_country_code
--       , sub_country_name
--       , delivery_instructions_1
--       , delivery_instructions_2
--       , delivery_refund_status_code
--       , delivery_status_code
--       , email
--       , first_name
--       , formal_salutation
--       , formatted_name
--       , informal_salutation
--       , last_name
--       , middle_name
--       , phone_country_code
--       , extension
--       , phone_display
--       , phone_number
--       , ticket_text
--       , will_call_name
--       , will_call_card_type_code
      , cast(delivery_method_id as int64) as delivery_method
--       , name_prefix_code
--       , name_suffix_code
      , transaction_id as transaction_id
--       , postal_code_display
      , attending_patron_account_id
--       , truncated_pan
--       , prefixed_truncated_pan
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'delivery') }}