select  service_charge_id as service_charge_id
    --   , created_by_operator_id
    --   , created_date
    --   , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , service_charge_code as service_charge_code
    --   , description
    --   , display_order
    --   , active
      , public_description as public_description
    --   , apply_manually
    --   , apply_to_comps
    --   , calculation_type_code
    --   , currency_code
    --   , editable
    --   , eligible_tax_exempt
    --   , include_in_price_type_code
      , include_in_ticket_price as include_in_ticket_price
    --   , max_cap_amount
    --   , min_cap_amount
    --   , refund_type_code
    --   , nearest_decimal_amount
    --   , rounding_direction_code
    --   , service_charge_type_code
    --   , is_tax
    --   , time_zone_code
    --   , fixed_end_date
    --   , fixed_start_date
    --   , end_time_unit_code
    --   , relative_end_time
    --   , relative_start_time
    --   , start_time_unit_code
    --   , delivery_method_id
    --   , print_service_charge_group_id
    --   , report_service_charge_group_id
    --   , ui_service_charge_group_id
    --   , service_charge_class_code
    --   , general_ledger_id
    --   , inclusive_tax_inherit
    --   , agency_org_group_id
    --   , inclusive_tax_id
    --   , one_time_use_limit
    --   , svc_chg_1xlmt_prd_type_code
    --   , one_time_limit_start_date
    --   , one_time_limit_end_date
    --   , one_time_limit_calendar_days
    --   , settlement_organization_id
    --   , refund_type_override_code
    --   , public_synopsis
    --   , short_public_description
              -- , timestamp_seconds(etl_batch_id) as etl_batch_ts
 
      
from    {{ source('wheelhouse_red_sox_ticketing', 'service_charge') }}