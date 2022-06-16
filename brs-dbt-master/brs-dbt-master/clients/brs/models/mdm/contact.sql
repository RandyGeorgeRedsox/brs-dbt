{{
    config(
        cluster_by=['_composite_key']
    )
}}

WITH provenue AS (

  SELECT *
  FROM {{ ref('provenue_contact') }} AS pv
  WHERE pv.email_recency_rank = 1 
   OR   pv.primary_rank = 1
)

, inactive_account AS (
  SELECT  account_id
  FROM    {{ ref('provenue_account') }}
  WHERE   is_active = 0
)


, new_contact AS (
SELECT
        TO_HEX(SHA1(CONCAT(IFNULL(sf.contact_id, ''), '_', IFNULL(CAST(pv.contact_id AS STRING), ''), '_', IFNULL(wh.email, '')))) AS _composite_key
      , CONCAT(COALESCE(sf.email, pv.email, wh.email), '_', pv.account_id) AS _composite_key_2
      , STRUCT(
            pv.contact_id
          , pv.account_id
          , pv.first_name
          , pv.last_name
          , pv.birthday
          , pv.title
          , pv.address
          , pv.phone
          , pv.email
          , pv.is_primary
          , pv.is_designated_likely_broker
          , pv.is_designated_sponsor
          , pv.is_designated_mlb
          , pv.is_designated_trade
          , pv.is_designated_internal
          , pv.csv_audiences
        ) AS provenue

      , STRUCT(
            sf.contact_id
          , sf.title
          , sf.account_id
          , sf.contact_name
          , sf.phone
          , sf.email
        ) AS salesforce

      , STRUCT(
            wh.email
          , wh.first_name
          , wh.last_name
          , wh.country
          , wh.state
          , wh.city
          , wh.zip
          , wh.address
          , wh.phone_number
          , wh.primary_ticket_account_id
          , wh.identity_point_id
          , wh.team_nickname
          , wh.guid
          , wh.interaction_count
          , wh.first_interaction_date
          , wh.last_interaction_date
          , wh.oneview_spend
          , wh.commerce_spend
          , wh.shop_spend
          , wh.stubhub_spend
          , wh.primary_ticketing_spend
          , wh.mlbtv_spend
          , wh.okta_id
          , wh.ticketing_rfm_score
          , wh.shop_rfm_score
          , wh.commerce_rfm_score
          , wh.total_rfm_score
          , wh.avidity_score
          , wh.avidity_classification
          , wh.mlbtv_ltv
          , wh.tickets_ltv
          , wh.shop_ltv
          , wh.fs_nonrenewal_probability
          , wh.total_tickets_purchased
          , wh.tickets_purchased_2016
          , wh.games_purchased_2016
          , wh.tickets_purchased_2017
          , wh.games_purchased_2017
          , wh.tickets_purchased_2018
          , wh.games_purchased_2018
          , wh.tickets_purchased_2019
          , wh.games_purchased_2019
          , wh.tickets_purchased_2020
          , wh.games_purchased_2020
          , wh.team_id
          , CAST(wh.distance_to_fenway AS FLOAT64) AS distance_to_fenway
          , wh.is_new_season_ticket_inquiries_lead
          , wh.is_group_tickets_lead
          , wh.is_fenway_premium_suites_lead
          , wh.is_mlb_brochure_download_lead
          , interaction_days_since_ticket_forwarding
          , interaction_days_since_commerce_billable_forms
          , interaction_days_since_account_linking
          , interaction_days_since_stubhub_order
          , interaction_days_since_shop
          , interaction_days_since_mlb_tv_usage
          , interaction_days_since_mlb_tv_sales
          , interaction_days_since_email_opened
          , interaction_days_since_qualtrics_voc
          , interaction_days_since_email_opened_iterable
          , interaction_days_since_qualtrics_market_tracker
          , interaction_days_since_optin_list_members
          , interaction_days_since_digital_ticket_scan
          , interaction_days_since_ballpark_views
          , interaction_days_since_clickstream_club_pages
          , interaction_days_since_forms_2
          , interaction_days_since_primary_ticket_order
          , interaction_days_since_ballpark_check_ins
          , interaction_days_since_form_survey
          , interaction_days_since_email_opened_zetahub
          , interaction_days_since_onesignal_ballpark_users
          , webform_submitted
          , tickets_purchased_stubhub_2017
          , tickets_purchased_stubhub_2018
          , tickets_purchased_stubhub_2019
          , tickets_purchased_stubhub_2020
          , newsletter_opt_in
          , ticket_guide_opt_in
          , last_ticket_purchase_platform
          , last_ticket_opponent_purchased
          , last_ticket_opponent_is_next_scheduled_opponent
          , ticket_purchase_to_next_scheduled_game
          , have_purchased_tickets_for_next_week
          , is_active_registered_user
          , is_newsletter_unsubscribed
          , last_email_campaign_interacted_with
          , last_email_opened
          , email_campaigns_received
          , email_campaigns_clicked
          , email_campaigns_opened
          , income
          , family_with_kids
          , kids_in_household
          , wh.csv_audiences
          , wh.paid_ads_placement_name
          , wh.is_paid_ads_lead
          , wh.promo
          , wh.form_submission_date
          , wh.latest_form_submission_date

        ) AS wheelhouse

        , STRUCT(
            fk.guardian_phone_cell
          , fk.guardian_email
          , fk.guardian_address_country
          , fk.guardian_address_zipcode
          , fk.guardian_address_state
          , fk.guardian_phone
          , fk.guardian_address_city
          , fk.guardian_address_street
          , fk.guardian_member_id
          , fk.guardian_lastname
          , fk.guardian_firstname
          , fk.guardian_dob
          , fk.guardian_title
          , IF(fk.guardian_email IS NOT NULL, TRUE, FALSE) AS is_kidnation_guardian
        ) AS fortress

      
FROM    provenue AS pv

        FULL OUTER JOIN {{ ref('salesforce_contact') }} AS sf
            ON pv.contact_id = sf.provenue_contact_id
        
        FULL OUTER JOIN {{ ref('wheelhouse_contact') }} AS wh
            ON pv.email = wh.email 
        
        LEFT OUTER JOIN {{ ref('fortress__kidnation__latest') }} AS fk
            ON pv.email = fk.email
)

, cte_a AS (

  SELECT  nc.* EXCEPT(_composite_key) 
      , COALESCE(c._composite_key, nc._composite_key) AS _composite_key
      
  FROM new_contact AS nc
       INNER JOIN `mdm.contact_key` AS c
          ON nc.provenue.contact_id = c.pv_contact_id
)

, cte_b AS (
SELECT  nc.* EXCEPT(_composite_key) 
      , COALESCE(c._composite_key, nc._composite_key) AS _composite_key

FROM new_contact AS nc
     INNER JOIN `mdm.contact_key` AS c
        ON nc.salesforce.contact_id = c.sf_contact_id
WHERE COALESCE(c._composite_key, nc._composite_key) NOT IN (SELECT _composite_key FROM cte_a)
)


, cte_c AS (
SELECT  nc.* EXCEPT(_composite_key) 
      , COALESCE(c._composite_key, nc._composite_key) AS _composite_key

FROM new_contact AS nc
     LEFT OUTER JOIN `mdm.contact_key` AS c
        ON nc.wheelhouse.email = c.wheelhouse_email
WHERE COALESCE(c._composite_key, nc._composite_key) NOT IN (SELECT _composite_key FROM cte_a UNION ALL SELECT _composite_key FROM cte_b)
)

SELECT * 
FROM (
  SELECT * FROM cte_a
  UNION ALL
  SELECT * FROM cte_b
  UNION ALL
  SELECT * FROM cte_c
) AS c
  LEFT OUTER JOIN inactive_account AS a
      ON c.provenue.account_id = a.account_id
WHERE a.account_id IS NULL



