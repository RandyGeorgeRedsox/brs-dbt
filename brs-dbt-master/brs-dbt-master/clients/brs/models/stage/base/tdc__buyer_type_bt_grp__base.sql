select  buyer_type_bt_grp_id as buyer_type_bt_grp_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
--       , display_order
      , buyer_type_id as buyer_type_id
      , buyer_type_group_id as buyer_type_group_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


FROM    {{ source('wheelhouse_red_sox_ticketing', 'buyer_type_bt_grp') }}
