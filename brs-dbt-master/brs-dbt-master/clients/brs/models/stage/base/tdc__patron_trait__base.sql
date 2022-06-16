select  patron_trait_id as patron_trait_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
--       , boolean_data
--       , currency_data
--       , date_data
--       , integer_data
--       , memo_data
--       , string_data
      , trait_id as trait_id
--       , last_updated_by_agency_id
--       , created_by_agency_id
--       , patron_contact_id
      , patron_account_id as account_id
--       , utility_run_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'patron_trait') }}