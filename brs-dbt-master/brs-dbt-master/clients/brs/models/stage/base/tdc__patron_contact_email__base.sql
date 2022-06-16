select  patron_contact_email_id as email_id
--       , created_by_operator_id as created_by_operator_id
--       , created_date as created_date
--       , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , if(primary > 0, true, false) as is_primary
      , lower(email) as email
--       , patron_email_type_code as patron_email_type_code
--       , email_format_type_code as email_format_type_code
--       , primary_billing as primary_billing
--       , access_code_id as access_code_id
--       , last_updated_by_agency_id as last_updated_by_agency_id
--       , created_by_agency_id as created_by_agency_id
      , patron_contact_id as contact_id
--       , patron_account_id as account_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'patron_contact_email')}}
