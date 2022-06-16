select  sales_rep_id as sales_rep_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
    --   , active
    --   , display_order
    --   , first_name
    --   , formal_salutation
      , formatted_name as formatted_name
    --   , informal_salutation
    --   , last_name
    --   , middle_name, name_prefix_code
    --   , name_suffix_code
    --   , sales_rep_group_id
      , sales_rep_sub_group_id as sales_rep_sub_group_id
    --   , etl_batch_id

from    {{ source('wheelhouse_red_sox_ticketing', 'sales_rep') }}



        