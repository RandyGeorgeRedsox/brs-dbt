{{
    config(
        cluster_by=['email']
    )
}}

WITH ticket_cnt AS(
      SELECT  source_financial_patron_id
            , SUM(quantity) AS total_tickets_purchased
      FROM   {{ ref('wheelhouse__ticket_details__latest') }} 
      GROUP BY source_financial_patron_id
)

,    ticket_game_by_year AS(
      SELECT  source_financial_patron_id 
            , SUM(quantity) AS tickets_purchased
            , COUNT(DISTINCT event_id) AS games_purchased
            , EXTRACT(YEAR FROM order_date) AS year
      FROM   {{ ref('wheelhouse__ticket_details__latest') }} 
      GROUP BY  source_financial_patron_id
              , EXTRACT(YEAR FROM order_date)
           )

,     cte_2016 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2016
             ,  games_purchased AS games_purchased_2016
       FROM ticket_game_by_year
       WHERE year = 2016)

,     cte_2017 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2017
             ,  games_purchased AS games_purchased_2017
       FROM ticket_game_by_year
       WHERE year = 2017)

,     cte_2018 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2018
             ,  games_purchased AS games_purchased_2018
       FROM ticket_game_by_year
       WHERE year = 2018)

,     cte_2019 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2019
             ,  games_purchased AS games_purchased_2019
       FROM ticket_game_by_year
       WHERE year = 2019)

,     cte_2020 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2020
             ,  games_purchased AS games_purchased_2020
       FROM ticket_game_by_year
       WHERE year = 2020)

,     cte_2021 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2021
             ,  games_purchased AS games_purchased_2021
       FROM ticket_game_by_year
       WHERE year = 2021)

,     cte_2022 AS(
       SELECT   source_financial_patron_id 
             ,  tickets_purchased AS tickets_purchased_2022
             ,  games_purchased AS games_purchased_2022
       FROM ticket_game_by_year
       WHERE year = 2022)


,     cte_fenway_distance AS (
            SELECT  contact_info.email AS email
                  , c.team_code
                  , c.distance AS distance_to_fenway
            FROM    {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST( contact_info.closest_ballpark ) AS c
            WHERE c.team_code = 'BOS' 
)

,     latest_form_submission_date AS (
            SELECT * EXCEPT(row_num)
            FROM (
              SELECT email
                   , form_submission_date AS latest_form_submission_date
                   , ROW_NUMBER() OVER(PARTITION BY email ORDER BY form_submission_date DESC) AS row_num
              FROM {{ ref('wheelhouse__paid_ads_lead_submissions__latest') }}
            )
            WHERE row_num = 1
)
  
,     paid_ads_lead_demo AS (
            SELECT  email
                  , TRUE AS is_paid_ads_lead
                  , IF(promo = 'Group Tickets', TRUE, FALSE) AS is_paid_ads_group_ticket_lead
                  , first_name
                  , last_name
                  , phone_number
                  , state
                  , street_address
                  , zip_code
                  , company
                   FROM    {{ ref('wheelhouse__paid_ads_lead_submissions__latest') }}
                 
)
,   paid_ads_lead_agg as (select email   , ARRAY_AGG(promo) AS promo
                  , ARRAY_AGG(placement_name) AS paid_ads_placement_name
                  , ARRAY_AGG(CAST(form_submission_date AS STRING)) AS form_submission_date
            FROM    {{ ref('wheelhouse__paid_ads_lead_submissions__latest') }}
            GROUP BY email)
, paid_ads_lead as (
      select 
       d.email
                  , d.is_paid_ads_lead
                  , d.is_paid_ads_group_ticket_lead
                  , d.first_name
                  , d.last_name
                  , d.phone_number
                  , d.state
                  , d.street_address
                  , d.zip_code
                  , d.company
                  , a.promo
                  , a.paid_ads_placement_name
                  , a.form_submission_date 
                  , row_number() over (partition by d.email) as row_num 
                  from
      paid_ads_lead_demo as d 
      left join paid_ads_lead_agg as a
      on a.email=d.email
      qualify row_num=1
)
,     cte_interactions AS (
            SELECT * EXCEPT (row_num) 
            FROM (
            SELECT  email
                  , data_source
                  , DATE_DIFF(CURRENT_DATE(),interaction_date, DAY) AS days_since_last_interaction
                  , ROW_NUMBER() OVER (PARTITION BY email, data_source ORDER BY interaction_date DESC) AS row_num
            FROM    {{ ref('wheelhouse__all_interactions') }}
            WHERE   email IS NOT NULL
            )
            WHERE row_num = 1
)

,     cte_interactions_agg AS (
            SELECT  email
                  , SUM(IF(data_source = "Onesignal Ballpark Users", days_since_last_interaction, NULL)) AS interaction_days_since_onesignal_ballpark_users
                  , SUM(IF(data_source = "Digital Ticket Scan", days_since_last_interaction, NULL)) AS interaction_days_since_digital_ticket_scan
                  , SUM(IF(data_source = "Clickstream Club Pages", days_since_last_interaction, NULL)) AS interaction_days_since_clickstream_club_pages
                  , SUM(IF(data_source = "Optin List Members", days_since_last_interaction, NULL)) AS interaction_days_since_optin_list_members
                  , SUM(IF(data_source = "Primary Ticket Order", days_since_last_interaction, NULL)) AS interaction_days_since_primary_ticket_order
                  , SUM(IF(data_source = "Commerce Billable Forms", days_since_last_interaction, NULL)) AS interaction_days_since_commerce_billable_forms
                  , SUM(IF(data_source = "MLB.TV Sales", days_since_last_interaction, NULL)) AS interaction_days_since_mlb_tv_sales
                  , SUM(IF(data_source = "Ballpark Views", days_since_last_interaction, NULL)) AS interaction_days_since_ballpark_views
                  , SUM(IF(data_source = "StubHub Order", days_since_last_interaction, NULL)) AS interaction_days_since_stubhub_order
                  , SUM(IF(data_source = "MLB.TV Usage", days_since_last_interaction, NULL)) AS interaction_days_since_mlb_tv_usage
                  , SUM(IF(data_source = "Qualtrics Market Tracker", days_since_last_interaction, NULL)) AS interaction_days_since_qualtrics_market_tracker
                  , SUM(IF(data_source = "Shop", days_since_last_interaction, NULL)) AS interaction_days_since_shop
                  , SUM(IF(data_source = "Email Opened", days_since_last_interaction, NULL)) AS interaction_days_since_email_opened
                  , SUM(IF(data_source = "Ballpark Check-Ins", days_since_last_interaction, NULL)) AS interaction_days_since_ballpark_check_ins
                  , SUM(IF(data_source = "Forms 2", days_since_last_interaction, NULL)) AS interaction_days_since_forms_2
                  , SUM(IF(data_source = "Email Opened - Iterable", days_since_last_interaction, NULL)) AS interaction_days_since_email_opened_iterable
                  , SUM(IF(data_source = "Account Linking", days_since_last_interaction, NULL)) AS interaction_days_since_account_linking
                  , SUM(IF(data_source = "Qualtrics VOC", days_since_last_interaction, NULL)) AS interaction_days_since_qualtrics_voc
                  , SUM(IF(data_source = "Form Survey", days_since_last_interaction, NULL)) AS interaction_days_since_form_survey
                  , SUM(IF(data_source = "Ticket Forwarding", days_since_last_interaction, NULL)) AS interaction_days_since_ticket_forwarding
                  , SUM(IF(data_source = "Email Opened - ZetaHub", days_since_last_interaction, NULL)) AS interaction_days_since_email_opened_zetahub
            FROM    cte_interactions 
            GROUP BY email
)

,     cte_submitted_form AS (
            SELECT   contact_info.email AS email
                   , ARRAY_AGG(f.title) AS webform_submitted
                   , SUM(CASE WHEN f.title = 'MLB - BOS - Group Ticket Information - 2019' THEN 1 ELSE 0 END) AS is_group_tickets_lead
                   , SUM(CASE WHEN f.title = 'MLB - BOS - Premium Suites Information - 2019' THEN 1 ELSE 0 END) AS is_fenway_premium_suites_lead
                   , SUM(CASE WHEN f.title = 'New Season Ticket Inquiries' OR f.title = 'BOS - New Season Tickets inquiry form 1.5 (limiting country choices)' THEN 1 ELSE 0 END) AS is_new_season_ticket_inquiries_lead
                   , SUM(CASE WHEN f.title = 'MLB - BOS - Group Ticket Brochure' THEN 1 ELSE 0 END) AS is_mlb_brochure_download_lead
            FROM     {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST (forms.submission_history) AS f
            WHERE f.title IS NOT NULL 
            GROUP BY 1

)
,     cte_oneview AS (
            SELECT   contact_info.email AS email
                   , DATE_DIFF(CURRENT_DATE(), summary.last_interaction.date, DAY) AS days_since_last_interaction
                   , shop.spend_total AS shop_spend
                   , data_science.total_rfm_score AS total_rfm_score_oneview
                   , a.avidity AS avidity_oneview
                   , email.newsletter AS newsletter_opt_in
                   , email.ticket_guide AS ticket_guide_opt_in
                   , IF(email.newsletter IS NULL, TRUE, FALSE) AS is_newsletter_unsubscribed
            FROM    {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST (data_science.team_avidity) AS a

)

,     email_campaign AS (
      SELECT email
           , ARRAY_AGG(DISTINCT CASE WHEN event_type = 'campaign_clicked' THEN campaign_name END IGNORE NULLS) AS email_campaigns_clicked
           , ARRAY_AGG(DISTINCT CASE WHEN event_type = 'campaign_opened' THEN campaign_name END IGNORE NULLS) AS email_campaigns_opened
           , ARRAY_AGG(DISTINCT CASE WHEN event_type = 'campaign_delivered' THEN campaign_name END IGNORE NULLS) AS email_campaigns_received
      FROM   {{ ref('wheelhouse__zetahub_events__latest') }}
      GROUP BY email 
)
,     cte_ticket_oneview AS (
            SELECT  email_id
                  , contact_info.email
                  , t.season
                  -- , t.tickets_purchased_pri AS total_ticket
                  , t.tickets_purchased_sh AS tickets_purchased_stubhub
            FROM    {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST(ticketing.history) AS t
)

      , cte_2017_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2017
            FROM    cte_ticket_oneview
            WHERE season = 2017
)
      , cte_2018_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2018
            FROM    cte_ticket_oneview
            WHERE season = 2018
)
      , cte_2019_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2019
            FROM    cte_ticket_oneview
            WHERE season = 2019
)
      , cte_2020_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2020
            FROM    cte_ticket_oneview
            WHERE season = 2020
)

      , cte_2021_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2021
            FROM    cte_ticket_oneview
            WHERE season = 2021
)

      , cte_2022_oneview AS (
            SELECT  email
                  , tickets_purchased_stubhub AS tickets_purchased_stubhub_2022
            FROM    cte_ticket_oneview
            WHERE season = 2022
)

      , registered_user_full AS (
            SELECT * EXCEPT(row_num)
            FROM (
                  SELECT  email_address AS email
                        , source
                        , is_active
                        , ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY is_active ASC) AS row_num
                  FROM {{ ref('wheelhouse__registered_user_full')}} 
            )
            WHERE row_num = 1
      )

SELECT  COALESCE(wh.email, pal.email) AS email
      , team_nickname
      , COALESCE(wh.first_name, pal.first_name) AS first_name
      , COALESCE(wh.last_name, pal.last_name) AS last_name
      , wh.country
      , COALESCE(wh.state, pal.state) AS state
      , wh.city
      , wh.zip
      , COALESCE(wh.address, pal.street_address) AS address
      , COALESCE(wh.phone_number, pal.phone_number) AS phone_number
      , identity_point_id
      , guid
      , interaction_count
      , first_interaction_date
      , last_interaction_date
      , oneview_spend
      , commerce_spend
      , o.shop_spend
      , stubhub_spend
      , primary_ticketing_spend
      , mlbtv_spend
      , okta_id
      , primary_ticket_account_id
      , ticketing_rfm_score
      , shop_rfm_score
      , commerce_rfm_score
      , total_rfm_score
      , avidity_score
      , avidity_classification
      , mlbtv_ltv
      , tickets_ltv
      , shop_ltv
      , fs_nonrenewal_probability
      , tc.total_tickets_purchased
      , IFNULL(tickets_purchased_2016,0) AS tickets_purchased_2016
      , IFNULL(games_purchased_2016,0) AS games_purchased_2016
      , IFNULL(tickets_purchased_2017,0) AS tickets_purchased_2017
      , IFNULL(games_purchased_2017,0) AS games_purchased_2017
      , IFNULL(tickets_purchased_2018,0) AS tickets_purchased_2018
      , IFNULL(games_purchased_2018,0) AS games_purchased_2018
      , IFNULL(tickets_purchased_2019,0) AS tickets_purchased_2019
      , IFNULL(games_purchased_2019,0) AS games_purchased_2019
      , IFNULL(tickets_purchased_2020,0) AS tickets_purchased_2020
      , IFNULL(games_purchased_2020,0) AS games_purchased_2020
      , IFNULL(tickets_purchased_2021,0) AS tickets_purchased_2021
      , IFNULL(games_purchased_2021,0) AS games_purchased_2021
      , IFNULL(tickets_purchased_2022,0) AS tickets_purchased_2022
      , IFNULL(games_purchased_2022,0) AS games_purchased_2022
      , team_id
      , IF(sf.is_group_tickets_lead > 0, TRUE, FALSE) AS is_group_tickets_lead
      , IF(sf.is_fenway_premium_suites_lead > 0, TRUE, FALSE) AS is_fenway_premium_suites_lead
      , IF(sf.is_new_season_ticket_inquiries_lead > 0, TRUE, FALSE) AS is_new_season_ticket_inquiries_lead
      , IF(sf.is_mlb_brochure_download_lead > 0, TRUE, FALSE) AS is_mlb_brochure_download_lead
      , o.newsletter_opt_in
      , o.ticket_guide_opt_in
      -- , IF(ca.is_new_season_ticket_inquiries_lead > 0, TRUE, FALSE) AS is_new_season_ticket_inquiries_lead
      -- , IF(ca.is_group_tickets_lead > 0, TRUE, FALSE) AS is_group_tickets_lead
      -- , IF(ca.is_fenway_premium_suites_lead > 0, TRUE, FALSE) AS is_fenway_premium_suites_lead
      -- , IF(ca.is_mlb_brochure_download_lead > 0, TRUE, FALSE) AS is_mlb_brochure_download_lead
      , fd.distance_to_fenway
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
      -- , o.days_since_last_interaction
      , sf.webform_submitted
      -- , total_rfm_score_oneview
      -- , avidity_oneview
      , IFNULL(tickets_purchased_stubhub_2017, 0) AS tickets_purchased_stubhub_2017
      , IFNULL(tickets_purchased_stubhub_2018, 0) AS tickets_purchased_stubhub_2018
      , IFNULL(tickets_purchased_stubhub_2019, 0) AS tickets_purchased_stubhub_2019
      , IFNULL(tickets_purchased_stubhub_2020, 0) AS tickets_purchased_stubhub_2020
      , IFNULL(tickets_purchased_stubhub_2021, 0) AS tickets_purchased_stubhub_2021
      , IFNULL(tickets_purchased_stubhub_2022, 0) AS tickets_purchased_stubhub_2022
      , last_ticket_purchase_platform
      , last_ticket_opponent_purchased
      , last_ticket_opponent_is_next_scheduled_opponent
      , ticket_purchase_to_next_scheduled_game
      , have_purchased_tickets_for_next_week
      , ru.is_active AS is_active_registered_user
      , o.is_newsletter_unsubscribed
      , le.last_email_campaign_interacted_with
      , le.last_email_opened
      , email_campaigns_received
      , email_campaigns_clicked
      , email_campaigns_opened
      , wad.income
      , wad.family_with_kids
      , wad.kids_in_household
      , csv.csv_audiences
      , pal.paid_ads_placement_name
      , pal.is_paid_ads_lead
      , pal.form_submission_date
      , pal.is_paid_ads_group_ticket_lead
      , pal.promo
      , lfs.latest_form_submission_date

FROM    {{ ref('wheelhouse__customer_summary_agg') }} wh
        LEFT OUTER JOIN ticket_cnt tc
              ON primary_ticket_account_id = tc.source_financial_patron_id
        LEFT OUTER JOIN cte_2016 c6
              ON primary_ticket_account_id = c6.source_financial_patron_id
        LEFT OUTER JOIN cte_2017 c7
              ON primary_ticket_account_id = c7.source_financial_patron_id
        LEFT OUTER JOIN cte_2018 c8
              ON primary_ticket_account_id = c8.source_financial_patron_id
        LEFT OUTER JOIN cte_2019 c9
              ON primary_ticket_account_id = c9.source_financial_patron_id
        LEFT OUTER JOIN cte_2020 c0
              ON primary_ticket_account_id = c0.source_financial_patron_id
        LEFT OUTER JOIN cte_2021 c21
              ON primary_ticket_account_id = c21.source_financial_patron_id
        LEFT OUTER JOIN cte_2022 c22
              ON primary_ticket_account_id = c22.source_financial_patron_id
      --   LEFT OUTER JOIN cte_campaign ca
      --         ON wh.email = ca.email
        LEFT OUTER JOIN cte_fenway_distance fd
              ON wh.email = fd.email
        LEFT OUTER JOIN cte_interactions_agg ia
              ON wh.email = ia.email
        LEFT OUTER JOIN cte_oneview AS o
              ON wh.email = o.email
        LEFT OUTER JOIN cte_submitted_form AS sf
              ON wh.email = sf.email
        LEFT OUTER JOIN cte_2017_oneview o7
            ON wh.email = o7.email
        LEFT OUTER JOIN cte_2018_oneview o8
            ON wh.email = o8.email
        LEFT OUTER JOIN cte_2019_oneview o9
            ON wh.email = o9.email
        LEFT OUTER JOIN cte_2020_oneview o0
            ON wh.email = o0.email
        LEFT OUTER JOIN cte_2021_oneview o21
            ON wh.email = o21.email
        LEFT OUTER JOIN cte_2022_oneview o22
            ON wh.email = o22.email
        LEFT OUTER JOIN {{ ref('last_ticket_purchase') }} AS l
            ON wh.email = l.email
        LEFT OUTER JOIN registered_user_full AS ru
            ON wh.email = ru.email
        LEFT OUTER JOIN {{ ref('last_email') }} AS le
            ON wh.email = le.email
        LEFT OUTER JOIN email_campaign AS ec
            ON wh.email = ec.email
        LEFT OUTER JOIN {{ ref('wheelhouse__acxiom_demographics__latest') }} AS wad
            ON wh.email = wad.email
        LEFT OUTER JOIN {{ ref('compiled_csv_uploads') }} AS csv
            ON wh.email = csv.email
        FULL OUTER JOIN paid_ads_lead AS pal
            ON wh.email = pal.email
        LEFT OUTER JOIN latest_form_submission_date AS lfs
            ON wh.email = lfs.email
