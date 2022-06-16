select  cast(patron_account_id as int64) as account_id
--       , created_by_operator_id as created_by_operator_id
      , created_date as created_ts
--       , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , patron_account_name as account_name
      , if(active > 0, true, false) as is_active 
--       , alternate_account_id as alternate_account_id
--       , encrypted_password as encrypted_password
--       , access_code_id as access_code_id
--       , last_updated_by_agency_id as last_updated_by_agency_id
--       , created_by_agency_id as created_by_agency_id
      , patron_account_type_code as account_type_code
--       , merge_as_target_only as merge_as_target_only
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'patron_account') }}