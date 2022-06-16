select  order_transaction_id as order_transaction_id
      -- , created_by_operator_id as created_by_operator_id
      , created_date as created_ts
      , order_trxn_assoc_type_code as order_trxn_assoc_type_code
      , order_id as order_id
      , transaction_id as transaction_id
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'order_transaction') }}