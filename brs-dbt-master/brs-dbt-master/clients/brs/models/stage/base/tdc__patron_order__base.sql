select  cast(order_id as int64) as order_id
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
--       , patron_address_type_code
--       , currency_code
      , cast(current_sales_balance as float64) as current_sales_balance 
--       , email
--       , patron_email_type_code
--       , invoicing_source_code
      , order_contents_type_code as order_contents_type_code 
      , order_date as order_date
--       , patron_contact_type_code
--       , first_name
--       , formal_salutation
      , formatted_name as patron_name
--       , informal_salutation
--       , last_name
--       , middle_name
--       , primary_phone_country_code
--       , primary_extension
--       , primary_phone_display
--       , primary_phone_number
--       , primary_phone_type_code
--       , reservation_revenue_amount
      , cast(sales_revenue_amount as float64) as sales_revenue_amount
--       , secondary_phone_country_code
--       , secondary_extension
--       , secondary_phone_display
--       , secondary_phone_number
--       , secondary_phone_type_code
--       , tax_exempt_id
--       , use_billing_info_on_file
--       , created_by_agency_id
      , cast(attending_patron_account_id as int64) as attending_account_id
      , cast(marketing_source_id as int64) as marketing_source_id
      , cast(financial_patron_account_id as int64) as financial_account_id
--       , name_prefix_code
--       , name_suffix_code
      , cast(sales_rep_id as int64) as provenue_sales_rep
      , service_rep_id as service_rep_id
--       , supplier_id
--       , postal_code_display
--       , opt_in_auto_consume_credit
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'patron_order') }}