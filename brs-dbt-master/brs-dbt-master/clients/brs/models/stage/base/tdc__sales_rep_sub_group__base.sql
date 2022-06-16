select  sales_rep_sub_group_id as sales_rep_sub_group_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
    --   , sales_rep_sub_group_code
      , description as description
    --   , display_order
    --   , active
    --   , sales_rep_group_id
    --   , etl_batch_id

from    {{ source('wheelhouse_red_sox_ticketing', 'sales_rep_sub_group') }}
