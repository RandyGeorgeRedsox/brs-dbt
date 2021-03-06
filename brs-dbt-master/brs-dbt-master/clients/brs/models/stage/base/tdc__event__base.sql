select  event_id as event_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , event_code as event_code
      , description as event_name
--       , display_order
--       , active
--       , public_description
--       , accept_data_last_modified_date
--       , accs_ctrl_enabled
--       , accs_ctrl_exit_scans_enabled
--       , accs_ctrl_last_print_dnld_date
--       , accs_ctrl_last_print_dnld_id
--       , accs_ctrl_end_time
--       , accs_ctrl_start_time
--       , accs_ctrl_end_time_unit_code
--       , accs_ctrl_relative_end_time
--       , accs_ctrl_relative_start_time
--       , accs_ctrl_start_time_unit_code
--       , barcode_prefix
--       , block_event_accept_to_pvo
--       , capacity
--       , cnsgnbck_sales_typ_restriction
--       , date_time_text
--       , donation_sales_typ_restriction
--       , entry_start_time
--       , entry_relative_start_time
--       , entry_start_time_unit_code
--       , event_class_code
      , event_date
--       , event_date_for_accss_ctxt_only
--       , event_date_type_code
--       , event_day_of_wk
--       , exchange_same_event_run
--       , exchange_same_primary_org
--       , exchange_same_season
--       , exchange_same_supplier
--       , exchange_same_venue
--       , event_rsv_rls_fixed_time
--       , event_inventory_type_code
--       , evt_rsv_rls_date_inherited
--       , evt_rsv_rls_relative_time
--       , evt_rsv_rls_time_unit_code
--       , event_status_code
--       , event_true_type_code
--       , evt_trx_rsv_rls_date_inherited
--       , evt_trx_rsv_rls_relative_time
--       , evt_trx_rsv_rls_time_unit_code
--       , event_url
--       , exchange_quantity_match_reqd
--       , external_barcode_mask
--       , external_synopsis
--       , externally_sell_by_type_code
--       , fixed_price_for_pri_auction
--       , flash_message
--       , group_rsv_rls_fixed_time
--       , group_price_structure_id
--       , grp_rsv_rls_date_inherited
--       , grp_rsv_rls_relative_time
--       , grp_rsv_rls_time_unit_code
--       , grp_trx_rsv_rls_date_inherited
--       , grp_trx_rsv_rls_relative_time
--       , grp_trx_rsv_rls_time_unit_code
--       , hold_code_inv_generated
--       , internal_synopsis
--       , packages_only
--       , price_structure_id
--       , primary_lia_same_event_run
--       , primary_lia_same_primary_org
--       , primary_lia_same_season
--       , primary_lia_same_supplier
--       , primary_lia_same_venue
--       , printing_status_code
--       , prohibit_accs_nonfully_paid_tx
--       , ranking_config_id
--       , recalc_event_seat_config_data
--       , reporting_capacity
--       , resale_sales_type_restriction
--       , running_time_in_minutes
--       , dflt_neighborhood_code
--       , dflt_neighborhood_desc
--       , dflt_neighborhood_pub_desc
--       , dflt_row
--       , dflt_seat_number
--       , dflt_section_code
--       , dflt_section_desc
--       , dflt_section_pub_desc
--       , secondary_lia_same_event_run
--       , secondary_lia_same_primary_org
--       , secondary_lia_same_season
--       , secondary_lia_same_supplier
--       , secondary_lia_same_venue
--       , secondary_title
--       , sold_out
--       , target_revenue_amount
--       , target_ticket_quantity
--       , text_line_1
--       , text_line_10
--       , text_line_2
--       , text_line_3
--       , text_line_4
--       , text_line_5
--       , text_line_6
--       , text_line_7
--       , text_line_8
--       , text_line_9
--       , transfer_sales_typ_restriction
--       , utility_execution_in_progress
--       , valid_scans_per_day
--       , valid_scans_per_event
--       , access_code_id
--       , associated_event_id
--       , cnsgn_bck_liability_type_code
--       , event_run_id
--       , group_format_id
--       , hold_code_config_id
--       , master_config_id
--       , price_scale_config_id
--       , primary_organization_id
--       , voucher_target_buyer_type_id
      , season_id as season_id
--       , external_liability_type_code
--       , secondary_liability_type_code
--       , reversal_liability_type_code
--       , secondary_organization_id
--       , secondary_scan_facility_id
--       , settlement_organization_id
--       , single_format_id
--       , supplier_id
--       , venue_id
--       , venue_config_id
--       , voucher_liability_type_code
--       , zone_extended_seat_config_id
--       , general_ledger_id
--       , event_category_id
--       , dyn_pricing_data_feed_enabled
--       , dyn_pricing_last_extract_date
--       , tixx_actvty_data_feed_enabled
--       , tixx_actvty_last_extract_date
--       , inclusive_tax_id
--       , self_srv_prt_styp_restriction
--       , percentage_limit_type_code
--       , capacity_percentage_limit
--       , forward_sales_type_restriction
--       , renewal_group_id
--       , can_be_released_online
--       , exchg_sales_type_restriction
--       , exchange_group_id
--       , ext_vchr_payment_method_id
--       , redemption_evt_grp_id
--       , replay_floor_price
--       , consign_back_exp_minutes
--       , donation_exp_minutes
--       , primary_auction_exp_minutes
--       , resale_exp_minutes
--       , resal_rllovr_cnsgnbk_exp_mins
--       , resal_rllovr_donation_exp_mins
--       , transfer_exp_minutes
--       , immediate_liability_type_code
--       , exchange_qty_gte_reqd
--       , ticket_quota
--       , external_receivable_type_id
--       , external_payment_plan_id
--       , short_public_description
--       , dflt_neighborhood_st_pub_desc
--       , dflt_neighborhood_pub_synopsis
--       , dflt_section_st_pub_desc
--       , dflt_section_pub_synopsis
--       , quota_extended_seat_config_id
--       , dflt_section_pub_code
--       , acquire_sales_type_restriction
--       , buyer_type_tkt_quota_enabled
--       , price_scale_tkt_quota_enabled
--       , table_seating_src_hold_code_id
--       , table_seating_tgt_hold_code_id
--       , table_seating_ext_seat_cfg_id
--       , date_display_class_code
--       , date_text
--       , time_text
--       , acc_ctl_sec_exit_scans_enabled
--       , valid_sec_scans_per_day
--       , valid_sec_scans_per_event
--       , acc_ctl_unpaid_ord_entry
--       , acc_ctl_unpaid_tkt_entry
--       , assign_friends_to_tix
--       , loyalty_program_id, loyalty_ext_seat_def_id
--       , consign_seller_retains_points
--       , manual_reoffer_retains_points
--       , pri_cus1_early_scan_qty
--       , pri_cus1_scan_point_qty
--       , pri_cus2_early_scan_qty
--       , pri_cus2_scan_point_qty
--       , pri_cus3_early_scan_qty
--       , pri_cus3_scan_point_qty
--       , pri_cus4_early_scan_qty
--       , pri_cus4_scan_point_qty
--       , pri_cus5_early_scan_qty
--       , pri_cus5_scan_point_qty
--       , pri_dac_early_scan_qty
--       , pri_dac_scan_point_qty
--       , pri_ext_early_scan_qty
--       , pri_ext_scan_point_qty
--       , pri_other_early_scan_qty
--       , pri_other_scan_point_qty
--       , pri_passbook_early_scan_qty
--       , pri_passbook_scan_qty
--       , pri_pur_account_map_type_code
--       , pri_pur_earned_type_code
--       , pri_pru_point_qualifier
--       , loyalty_round_direction_code
--       , single_sale_type_enabled
--       , group_sale_type_enabled
--       , package_sale_type_enabled
--       , pri_pru_group_multiplier
--       , pri_scan_account_map_type_code
--       , pri_tah_early_scan_qty
--       , pri_tah_scan_qty
--       , pri_tap_early_scan_qty
--       , pri_tap_scan_qty
--       , resale_seller_retains_points
--       , scan_early_arrival_mins
--       , sec_cus1_early_scan_qty
--       , sec_cus1_scan_point_qty
--       , sec_cus2_early_scan_qty
--       , sec_cus2_scan_point_qty
--       , sec_cus3_early_scan_qty
--       , sec_cus3_scan_point_qty
--       , sec_cus4_early_scan_qty
--       , sec_cus4_scan_point_qty
--       , sec_cus5_early_scan_qty
--       , sec_cus5_scan_point_qty
--       , sec_dac_early_scan_qty
--       , sec_dac_scan_point_qty
--       , sec_ext_early_scan_qty
--       , sec_ext_scan_point_qty
--       , sec_mpv_early_scan_qty
--       , sec_mpv_scan_point_qty
--       , sec_other_early_scan_qty
--       , sec_other_scan_point_qty
--       , sec_passbook_early_scan_qty
--       , sec_passbook_scan_qty
--       , sec_pur_account_map_type_code
--       , sec_pur_earned_type_code
--       , sec_pru_point_quantity
--       , sec_scan_account_map_type_code
--       , sec_tah_early_scan_qty
--       , sec_tah_scan_qty
--       , sec_tap_early_scan_qty
--       , sec_tap_scan_qty
--       , loyalty_program_qual_group_id
--       , membership_program_id
--       , mbrshp_prgm_act_sch_inherited
--       , mbrshp_prgm_act_start_time
--       , mbrshp_prgm_act_rel_start_time
--       , mbrshp_prgm_act_start_tim_unit
--       , mbrshp_prgm_exp_sch_inherited
--       , mbrshp_prgm_exp_end_time
--       , mbrshp_prgm_exp_rel_end_time
--       , mbrshp_prgm_exp_end_time_unit
--       , points_to_purchase_inherited
--       , ext_accessibility_type_code
--       , accessible_public_synopsis
--       , accessible_synopsis_inherited
--       , pri_google_pay_scan_qty
--       , pri_google_pay_early_scan_qty
--       , sec_google_pay_scan_qty
--       , sec_google_pay_early_scan_qty
--       , mbr_quota_ovrd_btps_quotas
--       , insurance_enabled
--       , hide_redemption_prices_ext
--       , offer_redemption_bt_exclsv
--       , exchange_value_gte_reqd
--       , pri_app_dac_bar_early_scan_qty
--       , pri_app_dac_nfc_early_scan_qty
--       , pri_apple_dac_nfc_scan_qty
--       , pri_apple_dac_bar_scan_qty
--       , pri_apple_nfc_early_scan_qty
--       , pri_apple_nfc_scan_qty
--       , sec_app_dac_bar_early_scan_qty
--       , sec_app_dac_nfc_early_scan_qty
--       , sec_apple_dac_nfc_scan_qty
--       , sec_apple_dac_bar_scan_qty
--       , sec_apple_nfc_early_scan_qty
--       , sec_apple_nfc_scan_qty
--       , pri_google_nfc_early_scan_qty
--       , pri_google_nfc_scan_qty
--       , pri_ggl_dac_bar_early_scan_qty
--       , pri_ggl_dac_nfc_early_scan_qty
--       , pri_google_dac_nfc_scan_qty
--       , pri_google_dac_bar_scan_qty
--       , sec_google_nfc_early_scan_qty
--       , sec_google_nfc_scan_qty
--       , sec_ggl_dac_bar_early_scan_qty
--       , sec_ggl_dac_nfc_early_scan_qty
--       , sec_google_dac_nfc_scan_qty
--       , sec_google_dac_bar_scan_qty
--       , inc_chg_stmt_option_code
--       , show_seat_location_in_stmt
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'event') }}