


SELECT  qualtrics_id
      , email
      , DATE(response_date) AS response_date
      , overall_numrat
      , pregame_plans_tailgated
      , pregame_plans_local_bar
      , pregame_plans_local_attraction
      , pregame_plans_bp
      , pregame_plans_eat
      , pregame_plans_merch
      , pregame_plans_avoid_lines
      , pregame_plans_other
      , pregame_plans_none
      , pregame_plans_otherspecify
      , pregame_plans_CHC_1
      , pregame_plans_CHC_2
      , pregame_plans_fu
      , pregame_plans_promo
      , attend_with_spouse
      , attend_with_adult_kids
      , attend_with_young_kids
      , attend_with_otherfam
      , attend_with_business
      , attend_with_friends
      , attend_with_alone
      , parking_numrat
      , seatview_numrat
      , Seatview_value
      , concess_screener
      , concess_no
      , concess_no_otherspecify
      , concess_numrat
      , concess_type_alcohol
      , concess_type_nonalcohol
      , concess_type_sandwich
      , concess_type_hotdog
      , concess_type_chicken
      , concess_type_pizza
      , concess_type_salad
      , concess_type_nachos
      , concess_type_snacks
      , concess_type_fries
      , concess_type_icecream
      , concess_type_other_dessert
      , concess_type_other_entree
      , concess_type_none
      , concess_quality_alcohol
      , concess_quality_nonalcohol
      , concess_quality_sandwich
      , concess_quality_hotdog
      , concess_quality_chicken
      , concess_quality_pizza
      , concess_quality_salad
      , concess_quality_nachos
      , concess_quality_snacks
      , concess_quality_fries
      , concess_quality_icecream
      , concess_quality_other_dessert
      , concess_quality_other_entree
      , concess_quality_none
      , Concess_spend
      , purchase_intent
      , incentives_question
      , incentives_ot
      , incentives_rank_price
      , incentives_rank_teamperform
      , incentives_rank_opponent
      , incentives_rank_weather
      , incentives_rank_vacation
      , incentives_rank_celebration
      , incentives_rank_giveaway
      , incentives_rank_theme
      , incentives_rank_friends_family
      , incentives_rank_discount
      , incentives_rank_advertisement
      , incentives_rank_socialmedia
      , incentives_rank_concess
      , baseball_engage
      , team_interest
      , team_favorite
      , team_avidity

-- SELECT  qualtrics_id
--       , response_date
--       -- , duration_seconds
--       -- , true_complete
--       -- , browser
--       -- , operating_system
--       -- , resolution
--       , email
--       -- , order_id
--       -- , web_order_id
--       -- , financial_id
--       -- , attending_id
--       -- , okta_id
--       -- , ticket_id
--       -- , vendor
--       -- , gamepk
--       -- , hometri
--       -- , awaytri
--       -- , game_shorthand
--       -- , stadium
--       -- , offer_type
--       -- , offer_name
--       -- , country
--       -- , state
--       -- , city
--       -- , age
--       -- , overall_numrat
--       -- , TOR_addon_1
--       -- , TOR_addon_2
--       -- , TOR_addon_3
--       -- , overall_numrat_ot
--       -- , HOU_addon_1
--       -- , attend_with_spouse
--       -- , attend_with_kids
--       -- , attend_with_otherfam
--       -- , attend_with_business
--       -- , attend_with_friends
--       -- , attend_with_alone
--       -- , travel_start_loc
--       -- , travel_start_zip
--       -- , travelto_method
--       -- , travelto_time
--       -- , travelto_time_expect
--       -- , parking_numrat
--       -- , parking_time
--       -- , parking_time_expect
--       -- , TB_addon_1
--       -- , TB_addon_2
--       -- , rule_aware
--       -- , ge_time
--       -- , ge_time_expect
--       -- , TB_addon_3
--       -- , TB_addon_4
--       -- , seatview_numrat
--       -- , seatview_expect
--       -- , seatview_value
--       -- , concess_screener
--       -- , concess_no
--       -- , concess_type_nonalcohol
--       -- , concess_type_alcohol
--       -- , concess_type_food
--       -- , concess_type_snack
--       -- , concess_type_dessert
--       -- , concess_type_other
--       -- , concess_numrat
--       -- , concess_location_mobiledelivery
--       -- , concess_location_mobilepickup
--       -- , concess_location_kiosk
--       -- , concess_location_stand
--       -- , concess_location_vendorstands
--       -- , concess_location_other
--       -- , MIN_addon_1
--       -- , concess_spend
--       -- , concess_spend_expect
--       -- , concess_wait
--       -- , concess_wait_expect
--       -- , concess_grid_fbquality
--       -- , concess_grid_selection
--       -- , concess_grid_price
--       -- , concess_grid_custserv
--       -- , concess_grid_clean
--       -- , concess_grid_distlines
--       -- , concess_grid_contactless
--       -- , concess_grid_mobile
--       -- , concess_grid_package
--       -- , concess_grid_access
--       -- , TB_addon_5
--       -- , TB_addon_6
--       -- , HOU_addon_2_1
--       -- , HOU_addon_2_2
--       -- , HOU_addon_2_3
--       -- , merch_screener
--       -- , merch_no
--       -- , merch_grid_merchquality
--       -- , merch_grid_selection
--       -- , merch_grid_price
--       -- , merch_grid_custserv
--       -- , merch_grid_access
--       -- , merch_grid_healthsafety
--       -- , TB_addon_7
--       -- , TB_addon_8
--       -- , MIN_addon_2
--       -- , TOR_addon_4
--       -- , TOR_addon_4_fu_1
--       -- , TOR_addon_4_fu_2
--       -- , TOR_addon_4_fu_3
--       -- , TOR_addon_4_fu_4
--       -- , TOR_addon_4_fu_5
--       -- , TOR_addon_4_fu_6
--       -- , TOR_addon_4_fu_7
--       -- , TOR_addon_4_fu_8
--       -- , TOR_addon_4_fu_9
--       -- , TOR_addon_4_fu_10
--       -- , TOR_addon_4_fu_11
--       -- , TOR_addon_4_fu_12
--       -- , TOR_addon_4_fu_13
--       -- , TOR_addon_4_fu_14
--       -- , TOR_addon_4_fu_15
--       -- , TOR_addon_4_fu_16
--       -- , TOR_addon_4_fu_17
--       -- , TOR_addon_4_fu_18
--       -- , TOR_addon_4_fu_19
--       -- , SD_addon_1_1
--       -- , SD_addon_1_2
--       -- , SD_addon_1_3
--       -- , SD_addon_1_4
--       -- , staff_grid_questions
--       -- , staff_grid_enforce
--       -- , staff_grid_compliant
--       -- , staff_grid_signage
--       -- , staff_grid_sanitizer
--       -- , staff_grid_feltsafe
--       -- , staff_grid_fu_entering
--       -- , staff_grid_fu_seat
--       -- , staff_grid_fu_walking
--       -- , staff_grid_fu_entertainment
--       -- , staff_grid_fu_concess
--       -- , staff_grid_fu_merch
--       -- , staff_grid_fu_restrooms
--       -- , staff_grid_fu_exiting
--       -- , staff_grid_fu_other
--       -- , TB_addon_9
--       -- , TB_addon_10
--       -- , TB_addon_11
--       -- , TB_addon_12
--       -- , compliance_facemask
--       -- , factor_safety_masks
--       -- , factor_safety_socialdistseats
--       -- , factor_safety_bags
--       -- , factor_safety_contactlesspay
--       -- , factor_safety_contactlessentry
--       -- , factor_safety_sanitizer
--       -- , factor_safety_socialdistlines
--       -- , factor_execution_masks
--       -- , factor_execution_socialdistseats
--       -- , factor_execution_bags
--       -- , factor_execution_contactlesspay
--       -- , factor_execution_contactlessentry
--       -- , factor_execution_sanitizer
--       -- , factor_execution_socialdistlines
--       -- , factor_execution_complete
--       -- , MIN_addon_3
--       -- , exit_stage
--       -- , exit_time
--       -- , exit_time_expect
--       -- , MIN_addon_9
--       -- , MIN_addon_4
--       -- , MIN_addon_5_1
--       -- , MIN_addon_5_2
--       -- , MIN_addon_5_3
--       -- , MIN_addon_5_4
--       -- , MIN_addon_5_5
--       -- , MIN_addon_5_6
--       -- , MIN_addon_5_7
--       -- , MIN_addon_6_1
--       -- , MIN_addon_6_2
--       -- , MIN_addon_6_3
--       -- , MIN_addon_6_4
--       -- , obq_ot
--       -- , TB_addon_13
--       -- , TB_addon_14
--       -- , TB_addon_15_1
--       -- , TB_addon_15_2
--       -- , TB_addon_15_3
--       -- , TB_addon_15_4
--       -- , TB_addon_15_5
--       -- , TB_addon_15_6
--       -- , TB_addon_15_7
--       -- , TB_addon_15_8
--       -- , TB_addon_15_9
--       -- , TB_addon_15_10
--       -- , TB_addon_15_11
--       -- , TB_addon_15_complete
--       -- , purchase_intent
--       -- , incentives_question
--       -- , incentives_ot
--       -- , incentives_grid_date
--       -- , incentives_grid_starttime
--       -- , incentives_grid_price
--       -- , incentives_grid_seatavail
--       -- , incentives_grid_opponent
--       -- , incentives_grid_weather
--       -- , incentives_grid_giveaways
--       -- , incentives_grid_vacation
--       -- , incentives_grid_celebration
--       -- , incentives_grid_teamperform
--       -- , incentives_grid_giveaway_specific
--       -- , incentives_grid_MIN
--       -- , incentives_grid_fu
--       -- , MIN_addon_7
--       -- , brandhealth_question
--       -- , brandhealth_grid_exciting
--       -- , brandhealth_grid_famfriendly
--       -- , brandhealth_grid_champion
--       -- , brandhealth_grid_posinfluence
--       -- , brandhealth_grid_welcome
--       -- , brandhealth_grid_emotional
--       -- , brandhealth_grid_trendy
--       -- , brandhealth_grid_diversity
--       -- , brandhealth_bene_lookforward
--       -- , brandhealth_bene_comfort
--       -- , brandhealth_bene_toughtimes
--       -- , brandhealth_bene_relax
--       -- , brandhealth_bene_mood
--       -- , brandhealth_bene_timeforself
--       -- , brandhealth_bene_emotion
--       -- , brandhealth_bene_family
--       -- , brandhealth_bene_spouse
--       -- , brandhealth_bene_friends
--       -- , brandhealth_bene_community
--       -- , brandhealth_bene_biggerthanself
--       -- , brandhealth_bene_valueschild
--       -- , brandhealth_bene_thingsself
--       -- , brandhealth_bene_placefrom
--       -- , brandhealth_bene_placelive
--       -- , brandhealth_bene_valuesimp
--       -- , brandhealth_bene_none
--       -- , brandhealth_chal_money
--       -- , brandhealth_chal_time
--       -- , brandhealth_chal_hasslegame
--       -- , brandhealth_chal_offseason
--       -- , brandhealth_chal_newstuff
--       -- , brandhealth_chal_toomanygames
--       -- , brandhealth_chal_access
--       -- , brandhealth_chal_findspaces
--       -- , brandhealth_chal_gamechanged
--       -- , brandhealth_chal_boring
--       -- , brandhealth_chal_noteam
--       -- , brandhealth_chal_teamperform
--       -- , brandhealth_chal_tradeplayers
--       -- , brandhealth_chal_keptuptimes
--       -- , brandhealth_chal_none
--       -- , brandhealth_complete
--       -- , MIN_addon_8
--       -- , SEA_addon_1
--       -- , sports_interest_profootball
--       -- , sports_interest_mensbasketball
--       -- , sports_interest_hockey
--       -- , sports_interest_baseball
--       -- , sports_interest_ussoccer
--       -- , sports_interest_intsoccer
--       -- , sports_interest_womensbasketball
--       -- , sports_interest_colfootball
--       -- , sports_interest_colbasketball
--       -- , sports_interest_esports
--       -- , sports_interest_golf
--       -- , sports_interest_boxing
--       -- , sports_interest_motor
--       -- , baseball_engage_watchtv
--       -- , baseball_engage_radio
--       -- , baseball_engage_news
--       -- , baseball_engage_standings
--       -- , baseball_engage_talkbaseball
--       -- , baseball_engage_onlinecommunity
--       -- , baseball_engage_play
--       -- , baseball_engage_fantasy
--       -- , baseball_engage_gamble
--       -- , baseball_engage_attendmilb
--       -- , baseball_engage_none
--       -- , SEA_addon_2
--       -- , SEA_addon_2_fu
--       -- , PIT_addon_1
--       -- , PIT_addon_2_1
--       -- , PIT_addon_2_2
--       -- , PIT_addon_2_3
--       -- , PIT_addon_2_4
--       -- , PIT_addon_2_5
--       -- , PIT_addon_2_fu
--       -- , PIT_addon_3
--       -- , PIT_addon_4_1
--       -- , PIT_addon_4_2
--       -- , PIT_addon_4_3
--       -- , PIT_addon_5_1
--       -- , PIT_addon_5_2
--       -- , PIT_addon_5_3
--       -- , PIT_addon_6_1
--       -- , PIT_addon_6_2
--       -- , PIT_addon_6_3
--       -- , PIT_addon_7_1
--       -- , PIT_addon_7_2
--       -- , PIT_addon_7_3
--       -- , PIT_addon_8_1
--       -- , PIT_addon_8_2
--       -- , PIT_addon_8_3
--       -- , PIT_addon_8_4
--       -- , PIT_addon_8_5
--       -- , PIT_addon_8_6
--       -- , BOS_addon_1
--       -- , BOS_addon_3
--       -- , BOS_addon_3_fu_1
--       -- , BOS_addon_3_fu_2_1
--       -- , BOS_addon_3_fu_2_2
--       -- , BOS_addon_3_fu_2_3
--       -- , BOS_addon_3_fu_3
--       -- , BOS_addon_4_1
--       -- , BOS_addon_4_2
--       -- , BOS_addon_4_3
--       -- , BOS_addon_4_4
--       -- , BOS_addon_4_5
--       -- , BOS_addon_5
--       -- , team_interest
--       -- , gender_id
--       -- , eth_id_native
--       -- , eth_id_asian
--       -- , eth_id_black
--       -- , eth_id_latinx
--       -- , eth_id_mideast
--       -- , eth_id_pacificisland
--       -- , eth_id_white
--       -- , eth_id_other
--       -- , eth_id_notsaying
--       -- , parent_id
--       -- , cih_id
--       -- , hhi_id
--       -- , polparty_id
--       -- , home_dist
--       -- , holiday
--       -- , COL_addon_1
--       -- , CWS_addon_1
--       -- , CWS_addon_2
--       -- , CWS_addon_3
--       -- , CWS_addon_4
--       -- , CWS_addon_5
--       -- , CWS_addon_6
--       -- , CWS_addon_7_1
--       -- , CWS_addon_7_2
--       -- , CWS_addon_7_3
--       -- , CWS_addon_8
--       -- , PHI_addon_1_1
--       -- , PHI_addon_1_2
--       -- , PHI_addon_1_3
--       -- , PHI_addon_1_4
--       -- , PHI_addon_1_5
--       -- , PHI_addon_2
--       -- , PHI_addon_3
--       -- , PHI_addon_4
--       -- , PHI_addon_5
--       -- , PHI_addon_6
--       -- , SEA_addon_3_1
--       -- , SEA_addon_3_2
--       -- , SEA_addon_3_3
--       -- , HOU_addon_3
--       -- , HOU_addon_4
--       -- , HOU_addon_5_1
--       -- , HOU_addon_5_2
--       -- , HOU_addon_5_3
--       -- , HOU_addon_5_4
--       -- , HOU_addon_5_5
--       -- , HOU_addon_5_6
--       -- , HOU_addon_5_7
--       -- , HOU_addon_5_8
--       -- , HOU_addon_5_9
--       -- , HOU_addon_5_10
--       -- , HOU_addon_5_11
--       -- , HOU_addon_5_12
--       -- , HOU_addon_5_13
--       -- , WAS_addon_1
--       -- , WAS_addon_2
--       -- , WAS_addon_3
--       -- , WAS_addon_4
--       -- , WAS_addon_5
--       -- , WAS_addon_6
--       -- , WAS_addon_7
--       -- , WAS_addon_8
--       -- , WAS_addon_9
--       -- , WAS_addon_10
--       -- , WAS_addon_11
--       -- , WAS_addon_12
--       -- , WAS_addon_13
--       -- , CLE_addon_1
      -- , CLE_addon_2
--       -- , CLE_addon_3
--       -- , CLE_addon_4_1
--       -- , CLE_addon_4_2
--       -- , CLE_addon_4_3
--       -- , PHI_addon_7
--       -- , NYM_addon_1
--       -- , MIN_addon_10
--       -- , MIN_addon_11
--       -- , MIN_addon_12
--       -- , incentives_grid_CWS
--       -- , overall_numrat_ot_parent_topics
--       -- , overall_numrat_ot_topics
--       -- , obq_ot_parent_topics
--       -- , obq_ot_topics
--       -- , incentives_ot_parent_topics
--       -- , incentives_ot_topics
--       -- , team_nickname
--       -- , team_id
--       -- , team_favorite
--       , overall_numrat
--       , IF(attend_with_spouse = 1.0, TRUE, FALSE) AS attend_with_spouse
--       , IF(attend_with_kids = 1.0, TRUE, FALSE) AS attend_with_kids
--       , IF(attend_with_otherfam = 1.0, TRUE, FALSE) AS attend_with_otherfam
--       , IF(attend_with_business = 1.0, TRUE, FALSE) AS attend_with_business
--       , IF(attend_with_friends = 1.0, TRUE, FALSE) AS attend_with_friends
--       , IF(attend_with_alone = 1.0, TRUE, FALSE) AS attend_with_alone
--       , parking_numrat
--       , seatview_numrat
--       , concess_numrat
--       , concess_spend
--       , purchase_intent
--       , incentives_grid_date
--       , incentives_grid_starttime
--       , incentives_grid_price
--       , incentives_grid_seatavail
--       , incentives_grid_opponent
--       , incentives_grid_weather
--       , incentives_grid_giveaways
--       , incentives_grid_vacation
--       , incentives_grid_celebration
--       , incentives_grid_teamperform
--       , incentives_grid_giveaway_specific
--       , PIT_addon_3
--       , team_interest
--       , team_favorite
--       , Pregame_plans
--       , PIT_addon_2
--       , Seatview_value
--       , Concess_type
--       , Concess_spend
--       , incentives_rank
--       , PIT_addon_4
--       , PIT_addon_5
--       , PIT_addon_6
--       , PIT_addon_7
--       , Baseball_engage
--       , team_avidity

FROM    {{ source('wheelhouse_red_sox', 'qualtrics_voc_post_attendance_2022_full') }} 