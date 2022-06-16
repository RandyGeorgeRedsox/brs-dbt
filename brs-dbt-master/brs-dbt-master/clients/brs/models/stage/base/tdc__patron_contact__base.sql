select  cast(patron_contact_id as int64) as contact_id
--       , created_by_operator_id as created_by_operator_id
--       , created_date as created_date
--       , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , if(primary > 0, true, false) as is_primary
      , first_name as first_name
      , last_name as last_name
      , formal_salutation as title
      , TIMESTAMP(date_of_birth) as birthday
      , formatted_name as formatted_name
--       , informal_salutation as informal_salutation
--       , middle_name as middle_name
--       , patron_contact_type_code as patron_contact_type_code
--       , name_prefix_code as name_prefix_code
--       , name_suffix_code as name_suffix_code
--       , access_code_id as access_code_id
--       , last_updated_by_agency_id as last_updated_by_agency_id
--       , created_by_agency_id as created_by_agency_id
      , cast(patron_account_id as int64) as account_name
--       , patron_contact_gndr_type_code as patron_contact_gndr_type_code
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'patron_contact') }}