select  package_id as package_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , package_code as package_code
      , description as description
--       , display_order
--       , active
--       , public_description
--       , accept_data_last_modified_date
--       , block_event_accept_to_pvo
--       , can_be_renewed
--       , package_status_code
--       , exchange_quantity_match_reqd
      , full_season_equiv as full_season_equiv
--       , liability_must_be_in_package
--       , liability_same_event_run
--       , liability_same_primary_org
--       , liability_same_season
--       , liability_same_series
--       , liability_same_supplier
--       , liability_same_venue
--       , package_class_code
--       , exchange_must_be_in_package
--       , exchange_same_event_run
--       , exchange_same_primary_org
--       , exchange_same_season
--       , exchange_same_supplier
--       , exchange_same_venue
--       , package_rsv_rls_fixed_time
--       , package_reference_date
--       , pkg_rsv_rls_date_inherited
--       , pkg_rsv_rls_relative_time
--       , pkg_rsv_rls_time_unit_code
--       , package_seat_calc_type
--       , pkg_trx_rsv_rls_date_inherited
--       , pkg_trx_rsv_rls_relative_time
--       , pkg_trx_rsv_rls_time_unit_code
--       , package_type_code
--       , printing_status_code
--       , prorate_fse
--       , ref_date_time_text
--       , rollover_only
--       , rounding_direction_code
--       , same_seat_quantity_required
--       , single_ticket_upsellable
--       , sold_out
--       , time_zone_code
--       , utility_execution_in_progress
--       , access_code_id
--       , primary_organization_id
--       , report_package_group_id
--       , season_id
--       , secondary_organization_id
--       , series_id
--       , settlement_organization_id
--       , subscriber_activity_since_id
--       , supplier_id
--       , renewal_group_id
--       , can_be_released_online
--       , exchange_qty_gte_reqd
--       , auto_renewal
--       , external_receivable_type_id
--       , external_payment_plan_id
--       , public_synopsis
--       , short_public_description
--       , date_display_class_code
--       , ref_date_text
--       , ref_time_text
--       , loyalty_program_id
--       , pri_pru_package_multiplier
--       , pri_pru_package_bonus
--       , pkg_loyalty_pgm_qual_group_id
--       , evt_loyalty_pgm_qual_group_id
--       , membership_program_id
--       , membership_level_id
--       , mbrshp_prgm_act_sch_inherited
--       , mbrshp_prgm_act_start_time
--       , mbrshp_prgm_act_rel_start_time
--       , mbrshp_prgm_act_start_tim_unit
--       , mbrshp_prgm_exp_sch_inherited
--       , mbrshp_prgm_exp_end_time
--       , mbrshp_prgm_exp_rel_end_time
--       , mbrshp_prgm_exp_end_time_unit
--       , evt_pur_qual_grp_sp_type_code
--       , evt_pur_qual_grp_lylt_pgm_id
--       , use_first_pl_venue_as_ref
--       , instructions
--       , mbrsh_lvl_mapping_type_code
--       , insurance_enabled
--       , exchange_value_gte_reqd
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

  
from    {{ source('wheelhouse_red_sox_ticketing', 'package') }}