select  cast(patron_contact_address_id as int64) as address_id
--       , created_by_operator_id as created_by_operator_id
--       , created_date as created_date
--       , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , if(primary > 0, true, false) as is_primary
      , addr1 as street_address
      , addr2 as street_address_line_2
      , city as city
      , country_code as country
--       , postal_code as postal_code
      , sub_country_code as state
--       , sub_country_name as sub_country_name
      , patron_address_type_code as address_type
      , if(primary_billing > 0, true, false) as is_primary_billing
--       , access_code_id as access_code_id
--       , last_updated_by_agency_id as last_updated_by_agency_id
--       , created_by_agency_id as created_by_agency_id
      , patron_contact_id as contact_id
      , patron_account_id as account_id
      , postal_code_display as postal_code_display
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'patron_contact_address') }}