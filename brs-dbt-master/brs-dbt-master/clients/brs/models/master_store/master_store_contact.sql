{{
    config(
        materialized= 'table'
    )
}}

WITH  orders_cte AS (
      SELECT  pv_account_id
            , MIN(DATE(created_date)) AS first_purchase_date
            , MAX(DATE(created_date)) AS last_purchase_date
            , COUNT(DATE(created_date)) AS lifetime_orders

      FROM    {{ ref('master_store_transaction') }} 
      WHERE   num_tickets > 0
      GROUP BY pv_account_id
)
, ten_tickets_last30d AS (
      SELECT  DISTINCT pv_account_id
            , TRUE AS ten_tickets_1txn_1game_last30d        
      
      FROM {{ ref('game_transaction') }}
      WHERE num_tickets > 10 AND DATE_DIFF(CURRENT_DATE(),created_date, DAY) <= 30  
)

, last_order AS (
      SELECT  pv_account_id
            , ticket_spending AS last_ticket_order_purchase_value
      FROM (
            SELECT  pv_account_id
                  , ticket_spending
                  , ROW_NUMBER() OVER (PARTITION BY pv_account_id ORDER BY created_date DESC) AS row_num
            FROM    {{ ref('master_store_transaction') }}
            )
      WHERE  row_num = 1  
)

, purchase_week AS (
      SELECT * EXCEPT(row_num) 
      FROM (
            SELECT  pv_account_id
                  , week_of_year AS most_often_ticket_purchased_event_week
                  , num_games AS most_often_ticket_purchased_event_week_event_count
                  , ROW_NUMBER() OVER (PARTITION BY pv_account_id ORDER BY num_games DESC) AS row_num
            FROM    {{ ref('master_store_transaction') }}
)
      WHERE row_num = 1 

)

,  order_btg AS (
      SELECT  account_name
            , buyer_type_code
            , ARRAY_AGG(
                  DISTINCT CONCAT(EXTRACT(YEAR FROM order_start_date), '-', buyer_type_group_code) 
                  ) AS buyer_type_group
      FROM    {{ ref('order') }}
      WHERE   buyer_type_group_code IS NOT NULL
      GROUP BY account_name
             , buyer_type_code
)


,     cte AS(

      SELECT  a.provenue.primary_contact.contact_id AS pv_contact_id
            , con.salesforce.contact_id AS sf_contact_id
            , con._composite_key
            , a.provenue.account_id AS pv_account_id
            , CAST(a.provenue.created_ts AS DATE) AS pv_account_created_date
            , CONCAT(IFNULL(CAST(a.provenue.account_id AS STRING), ''), '_', IFNULL(con.provenue.email, '')) AS account_email
            , o.first_purchase_date
            , o.last_purchase_date
            , lo.last_ticket_order_purchase_value
            , CAST(lifetime_orders / (CASE WHEN DATE_DIFF(CURRENT_DATE, CAST(a.provenue.created_ts AS DATE), YEAR) = 0 THEN 1 ELSE DATE_DIFF(CURRENT_DATE, CAST(a.provenue.created_ts AS DATE), YEAR) END) AS INT64) AS average_purchase_frequency 
            , CAST(a.provenue.is_active AS BOOL) AS is_routed_to_salesforce
            , CAST(a.provenue.current_red_sox_season_ticket_holder AS BOOL) AS current_season_ticket_holder
            , CAST(a.provenue.lapsed_red_sox_season_ticket_holder AS BOOL) AS lapsed_season_ticket_holder
            , CAST(a.provenue.current_red_sox_parital_plan_buyer AS BOOL) AS current_partial_plan_buyer
            , CAST(a.provenue.lapsed_red_sox_partial_plan_buyer AS BOOL) AS lapsed_partial_plan_buyer
            , CAST(a.provenue.current_red_sox_group_buyer AS BOOL) AS current_group_buyer
            , CAST(a.provenue.lapsed_red_sox_group_buyer AS BOOL) AS lapsed_group_buyer
            , a.provenue.has_reserved_season_tickets AS has_reserved_season_tickets
            , ob.buyer_type_group
            , ob.buyer_type_code
            , ad.country AS pv_country
            , CAST(a.provenue.total_spend AS FLOAT64) AS total_lifetime_spend
            , CAST(a.provenue.package_spend AS FLOAT64) AS current_year_package_tickets_spend
            , CAST(a.provenue.group_spend AS FLOAT64) AS current_year_group_tickets_spend
            , CAST(a.provenue.single_spend AS FLOAT64) AS current_year_single_tickets_spend
            , CAST(a.provenue.last_year_spend AS FLOAT64) AS last_year_total_tickets_spend
            , DATE_DIFF(CURRENT_DATE(),con.wheelhouse.last_interaction_date, DAY) AS days_since_last_interaction
            , CAST(con.wheelhouse.shop_spend AS FLOAT64) AS shop_spend
            , CAST(con.wheelhouse.commerce_spend AS FLOAT64) AS commerce_spend
            , CAST(con.wheelhouse.mlbtv_spend AS FLOAT64) AS mlbtv_spend
            , CAST(con.wheelhouse.oneview_spend AS FLOAT64) AS oneview_spend
            , con.wheelhouse.total_tickets_purchased
            , IFNULL(con.wheelhouse.tickets_purchased_2016,0) AS tickets_purchased_2016
            , IFNULL(con.wheelhouse.games_purchased_2016,0) AS games_purchased_2016
            , IFNULL(con.wheelhouse.tickets_purchased_2017,0) AS tickets_purchased_2017
            , IFNULL(con.wheelhouse.games_purchased_2017,0) AS games_purchased_2017
            , IFNULL(con.wheelhouse.tickets_purchased_2018,0) AS tickets_purchased_2018
            , IFNULL(con.wheelhouse.games_purchased_2018,0) AS games_purchased_2018
            , IFNULL(con.wheelhouse.tickets_purchased_2019,0) AS tickets_purchased_2019
            , IFNULL(con.wheelhouse.games_purchased_2019,0) AS games_purchased_2019
            , IFNULL(con.wheelhouse.tickets_purchased_2020,0) AS tickets_purchased_2020
            , IFNULL(con.wheelhouse.games_purchased_2020,0) AS games_purchased_2020
            , con.wheelhouse.is_new_season_ticket_inquiries_lead
            , con.wheelhouse.is_group_tickets_lead
            , con.wheelhouse.is_fenway_premium_suites_lead
            , con.wheelhouse.is_mlb_brochure_download_lead
            , con.provenue.is_designated_likely_broker
            , con.provenue.is_designated_sponsor
            , con.provenue.is_designated_mlb
            , con.provenue.is_designated_trade
            , con.provenue.is_designated_internal
            , COALESCE(con.provenue.first_name, wheelhouse.first_name) AS first_name
            , COALESCE(con.provenue.last_name, wheelhouse.last_name) AS last_name
            , ad.city
            , ad.state
            , ad.postal_code_display AS zip
            , COALESCE(con.provenue.email, con.wheelhouse.email) AS email
            , COALESCE(ph.phone_number, con.wheelhouse.phone_number) AS phone_number
            , ph.phone_number AS provenue_phone_number
            , con.wheelhouse.phone_number AS wheelhouse_phone_number
            , ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(con.provenue.csv_audiences, con.wheelhouse.csv_audiences)) x) AS csv_audiences
            , con.wheelhouse.ticketing_rfm_score
            , con.wheelhouse.shop_rfm_score
            , con.wheelhouse.commerce_rfm_score
            , con.wheelhouse.total_rfm_score
            , con.wheelhouse.avidity_score
            , con.wheelhouse.avidity_classification
            , 'tbd' AS company
            , con.wheelhouse.distance_to_fenway
            , con.wheelhouse.interaction_days_since_ticket_forwarding
            , con.wheelhouse.interaction_days_since_commerce_billable_forms
            , con.wheelhouse.interaction_days_since_account_linking
            , con.wheelhouse.interaction_days_since_stubhub_order
            , con.wheelhouse.interaction_days_since_shop
            , con.wheelhouse.interaction_days_since_mlb_tv_usage
            , con.wheelhouse.interaction_days_since_mlb_tv_sales
            , con.wheelhouse.interaction_days_since_email_opened
            , con.wheelhouse.interaction_days_since_qualtrics_voc
            , con.wheelhouse.interaction_days_since_email_opened_iterable
            , con.wheelhouse.interaction_days_since_qualtrics_market_tracker
            , con.wheelhouse.interaction_days_since_optin_list_members
            , con.wheelhouse.interaction_days_since_digital_ticket_scan
            , con.wheelhouse.interaction_days_since_ballpark_views
            , con.wheelhouse.interaction_days_since_clickstream_club_pages
            , con.wheelhouse.interaction_days_since_forms_2
            , con.wheelhouse.interaction_days_since_primary_ticket_order
            , con.wheelhouse.interaction_days_since_ballpark_check_ins
            , con.wheelhouse.interaction_days_since_form_survey
            , con.wheelhouse.interaction_days_since_email_opened_zetahub
            , con.wheelhouse.interaction_days_since_onesignal_ballpark_users
            , con.wheelhouse.webform_submitted
            , IFNULL(con.wheelhouse.tickets_purchased_stubhub_2017, 0) AS tickets_purchased_stubhub_2017
            , IFNULL(con.wheelhouse.tickets_purchased_stubhub_2018, 0) AS tickets_purchased_stubhub_2018
            , IFNULL(con.wheelhouse.tickets_purchased_stubhub_2019, 0) AS tickets_purchased_stubhub_2019
            , IFNULL(con.wheelhouse.tickets_purchased_stubhub_2020, 0) AS tickets_purchased_stubhub_2020
            , con.wheelhouse.newsletter_opt_in
            , con.wheelhouse.ticket_guide_opt_in
            , a.salesforce.days_since_closed_lost
            , con.wheelhouse.last_ticket_purchase_platform
            , con.wheelhouse.last_ticket_opponent_purchased
            , con.wheelhouse.last_ticket_opponent_is_next_scheduled_opponent
            , con.wheelhouse.ticket_purchase_to_next_scheduled_game
            , con.wheelhouse.have_purchased_tickets_for_next_week
            , con.wheelhouse.is_active_registered_user
            , con.wheelhouse.is_newsletter_unsubscribed
            , con.wheelhouse.paid_ads_placement_name
            , IFNULL(con.wheelhouse.is_paid_ads_lead, FALSE) AS is_paid_ads_lead
            , con.wheelhouse.promo
            , con.wheelhouse.form_submission_date
            , con.wheelhouse.latest_form_submission_date
            , IF(g.ten_tickets_1txn_1game_last30d IS TRUE, TRUE, FALSE) AS ten_tickets_1txn_1game_last30d
            , pw.most_often_ticket_purchased_event_week
            , pw.most_often_ticket_purchased_event_week_event_count
            , IF(EXTRACT(WEEK FROM CURRENT_DATE())+1 = pw.most_often_ticket_purchased_event_week, TRUE, FALSE) AS next_week_is_most_often_purchased_tickets_week
            , con.wheelhouse.last_email_opened
            , con.wheelhouse.last_email_campaign_interacted_with
            , con.wheelhouse.email_campaigns_received
            , con.wheelhouse.email_campaigns_clicked
            , con.wheelhouse.email_campaigns_opened
            , a.provenue.is_merged
            , con.wheelhouse.income AS acxiom_income
            , con.wheelhouse.family_with_kids AS acxiom_family_with_kids
            , con.wheelhouse.kids_in_household AS acxiom_kids_in_household
            , (3.092396369 + 0.00554994 * a.provenue.plan_event_total_spend + 10.04760847 * con.wheelhouse.avidity_score - 0.048000816 * con.wheelhouse.distance_to_fenway) AS homestand_buyer_score
            , a.provenue.is_active
            , ROW_NUMBER() OVER(PARTITION BY con._composite_key ORDER BY ad.city DESC) AS row_num
            , IF(COALESCE(con.provenue.email, con.wheelhouse.email) IN (SELECT DISTINCT LOWER(email) FROM {{ ref('nyy_targeted')}}), TRUE, FALSE) AS is_202106_nyy
            , IF(COALESCE(con.provenue.email, con.wheelhouse.email) IN (SELECT DISTINCT LOWER(email) FROM {{ ref('pedroia_targeted')}}), TRUE, FALSE) AS is_202106_pedroia
            , IF(COALESCE(con.provenue.email, con.wheelhouse.email) IN (SELECT DISTINCT LOWER(email) FROM {{ ref('fb_leads_exclusion')}}), TRUE, FALSE) AS is_fb_leads_exclusion
            

      FROM    {{ ref('contact') }} con
              LEFT OUTER JOIN UNNEST( provenue.address ) ad
              LEFT OUTER JOIN UNNEST( provenue.phone ) ph
            --   LEFT OUTER JOIN UNNEST(provenue.csv_audiences) pv_csv
            --   LEFT OUTER JOIN UNNEST(wheelhouse.csv_audiences) wh_csv
                  FULL OUTER JOIN {{ ref('account') }} a
                        ON con.provenue.account_id = a.provenue.account_id
              LEFT OUTER JOIN orders_cte o
                  ON con.provenue.account_id = o.pv_account_id
              LEFT OUTER JOIN last_order AS lo
                  ON con.provenue.account_id = lo.pv_account_id
              LEFT OUTER JOIN ten_tickets_last30d AS g
                  ON con.provenue.account_id = g.pv_account_id
              LEFT OUTER JOIN purchase_week AS pw
                  ON con.provenue.account_id = pw.pv_account_id
              LEFT OUTER JOIN order_btg ob
                  ON con.provenue.account_id = ob.account_name

               

      WHERE   ad.is_primary = TRUE
      AND     con.provenue.is_primary = TRUE
      OR      (ARRAY_LENGTH(con.provenue.csv_audiences) > 0 OR ARRAY_LENGTH(con.wheelhouse.csv_audiences) > 0)
      OR      con.wheelhouse.is_paid_ads_lead IS TRUE -- adding contacts from wheelhouse paid_ads_lead only

)

SELECT * EXCEPT(row_num)
FROM   cte
WHERE  row_num = 1
