select  cast(patron_contact_phone_id as int64) as phone_id
--       , created_by_operator_id as created_by_operator_id
--       , created_date as created_date
--       , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , if(primary > 0, true, false) as is_primary
      , country_code as country_code
      , extension as extension
--       , phone_display as phone_display
      , phone_number as phone_number
      , patron_phone_type_code as patron_phone_type_code
--       , if(secondary > 0, true, false) as is_secondary
--       , access_code_id as access_code_id
--       , last_updated_by_agency_id as last_updated_by_agency_id
--       , created_by_agency_id as created_by_agency_id
      , patron_contact_id as contact_id
--       , patron_account_id as account_id
      , if(mobile > 0, true, false) as is_mobile
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'patron_contact_phone') }}