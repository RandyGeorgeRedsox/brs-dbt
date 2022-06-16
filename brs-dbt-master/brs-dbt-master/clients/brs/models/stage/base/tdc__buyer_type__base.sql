select  buyer_type_id as buyer_type_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , buyer_type_code as buyer_type_code
      , description as buyer_type
--       , display_order
--       , active
      , public_description as buyer_type_public_description
--       , capacity_control_type_code
--       , default_capacity_control_limit
--       , display_indicator
--       , editable
--       , full_price
--       , max_offer_tixx_limit
--       , min_offer_tixx_limit
--       , price_to_print_text
--       , requires_note
--       , restrict_consign_backs
--       , restrict_donations
--       , restrict_resales
--       , restrict_transfers
--       , selectable
--       , system_selectable
--       , tax_exempt
      , buyer_type_group_id as buyer_type_group
--       , promotion_id
      , report_buyer_type_group_id as report_buyer_type_group_id
--       , supplier_id
--       , qty_relationship_description
--       , qty_relationship_error_message
--       , buyer_type_qty_rel_val_rl_code
--       , restrict_self_service_prints
--       , extended_seat_definition_id
--       , ext_seat_restriction_pub_desc
--       , restrict_forward
--       , enforce_price_str_valid_dates
--       , restrict_exchange
--       , voucher_redemption
--       , public_synopsis
--       , short_public_description
--       , restrict_acquire
--       , ticket_quota
--       , sale_upper_edit_limit_type
--       , sale_lower_edit_limit_type
--       , resv_upper_edit_limit_type
--       , resv_lower_edit_limit_type
--       , sale_upper_edit_limit
--       , sale_lower_edit_limit
--       , resv_upper_edit_limit
--       , resv_lower_edit_limit
--       , activate_mbrshp_immediately
--       , mbrshp_prgm_exp_sch_inherited
--       , mbrshp_prgm_exp_end_time_unit
--       , mbrshp_prgm_exp_rel_end_time
--       , restrict_relocation
--       , retain_access_relocation
--       , retain_access_event_exchange
      --       -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'buyer_type') }}