select  hold_code_id as hold_code_id
      , created_by_operator_id as created_by_operator_id
      , created_date as created_ts
      , last_updated_by_operator_id as last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , hold_code as hold_code
      , description as description
      , display_order as display_order
      , active as active
      , display_indicator as display_indicator
      , exclude_from_report_capacity as exclude_from_report_capacity
      , hold_code_type_code as hold_code_type_code
      , hold_code_group_id as hold_code_group_id
      , supplier_id as supplier_id
from    {{ source('wheelhouse_red_sox_ticketing', 'hold_code') }}