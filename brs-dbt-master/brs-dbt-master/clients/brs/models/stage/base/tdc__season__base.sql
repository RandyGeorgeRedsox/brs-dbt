select  season_id as season_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , season_code as season_code
--       , description
--       , display_order
--       , active
--       , evt_rsv_rls_date_inherited
--       , evt_rsv_rls_relative_time
--       , evt_rsv_rls_time_unit_code
--       , evt_trx_rsv_rls_date_inherited
--       , evt_trx_rsv_rls_relative_time
--       , evt_trx_rsv_rls_time_unit_code
--       , grp_rsv_rls_date_inherited
--       , grp_rsv_rls_relative_time
--       , grp_rsv_rls_time_unit_code
--       , grp_trx_rsv_rls_date_inherited
--       , grp_trx_rsv_rls_relative_time
--       , grp_trx_rsv_rls_time_unit_code
--       , pkg_rsv_rls_date_inherited
--       , pkg_rsv_rls_relative_time
--       , pkg_rsv_rls_time_unit_code
--       , pkg_trx_rsv_rls_date_inherited
--       , pkg_trx_rsv_rls_relative_time
--       , pkg_trx_rsv_rls_time_unit_code
--       , season_end_date
--       , season_start_date
--       , season_status_code
--       , subscriber_since_date
--       , subscription_end_date
--       , subscription_start_date
--       , cnsgn_bck_liability_type_code
--       , default_delivery_method_id
--       , season_type_id
--       , external_liability_type_code
--       , secondary_liability_type_code
--       , reversal_liability_type_code
--       , supplier_id
--       , voucher_liability_type_code
--       , replay_floor_price
--       , consign_back_exp_minutes
--       , donation_exp_minutes
--       , primary_auction_exp_minutes
--       , resale_exp_minutes
--       , resal_rllovr_cnsgnbk_exp_mins
--       , resal_rllovr_donation_exp_mins
--       , transfer_exp_minutes
--       , immediate_liability_type_code
--       , external_receivable_type_id
--       , external_payment_plan_id
--       , points_to_purchase_inherited
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts


from    {{ source('wheelhouse_red_sox_ticketing', 'season') }}