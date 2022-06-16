select  buyer_type_group_id as buyer_type_group_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , buyer_type_group_code as buyer_type_group_code
      , description as description 
--       , display_order
--       , ui_group
--       , display_indicator
--       , report_filter_group
--       , report_output_group
--       , supplier_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'buyer_type_group') }}


