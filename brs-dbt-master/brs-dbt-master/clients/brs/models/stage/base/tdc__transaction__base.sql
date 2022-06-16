select  transaction_id as transaction_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
--       , cart_id
--       , external_transaction_id
--       , transaction_contents_type_code
      , transaction_date as transaction_date
--       , transaction_note_text
--       , utility_type_code
      , agency_id as agency_id
      , channel_id as channel_id
      , market_offer_id as market_offer_id
--       , operator_id
--       , station_id
--       , transaction_note_type_id
--       , utility_operator_id
--       , transaction_patron_account_id
--       , send_confirmation_email
--       , utility_run_id
--       , trxn_insurance_status_code
--       , insurance_provider_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'transaction') }}