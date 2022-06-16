select  delivery_method_id as delivery_method_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , delivery_method_code as  delivery_method_code
      , description as description
--       , display_order
--       , active
--       , public_description
--       , delivery_type_code
--       , time_zone_code
--       , fixed_end_date
--       , end_time_unit_code
--       , relative_end_time
--       , relative_start_time
--       , start_time_unit_code
--       , public_synopsis
--       , short_public_description
      -- , etl_batch_id as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'delivery_method') }}