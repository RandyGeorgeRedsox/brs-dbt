{{
    config(
        cluster_by=['email']
    )
}}

WITH  cte_fenway_distance AS (
            SELECT  contact_info.email AS email
                  , c.team_code
                  , c.distance AS distance_to_fenway
            FROM    {{ ref('wheelhouse__oneview') }}
            CROSS JOIN UNNEST( contact_info.closest_ballpark ) AS c
            WHERE c.team_code = 'BOS' 
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
                   , shop.spend_total AS shop_spend_oneview
                   , data_science.total_rfm_score AS total_rfm_score_oneview
                   , a.avidity AS avidity_oneview
            FROM    {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST (data_science.team_avidity) AS a

)

,     cte_ticket_oneview AS (
            SELECT  email_id
                  , contact_info.email
                  , t.season
                  , (t.tickets_purchased_pri + t.tickets_purchased_sh) AS total_ticket
            FROM    {{ ref('wheelhouse__oneview') }}
            LEFT OUTER JOIN UNNEST(ticketing.history) AS t
            -- WHERE contact_info.email = 'willsky@mit.edu'
)

      , cte_2017_oneview AS (
            SELECT  email
                  , total_ticket AS total_ticket_2017
            FROM    cte_ticket_oneview
            WHERE season = 2017
)
      , cte_2018_oneview AS (
            SELECT  email
                  , total_ticket AS total_ticket_2018
            FROM    cte_ticket_oneview
            WHERE season = 2018
)
      , cte_2019_oneview AS (
            SELECT  email
                  , total_ticket AS total_ticket_2019
            FROM    cte_ticket_oneview
            WHERE season = 2019
)
      , cte_2020_oneview AS (
            SELECT  email
                  , total_ticket AS total_ticket_2020
            FROM    cte_ticket_oneview
            WHERE season = 2020
)

SELECT  co.email
      , o.days_since_last_interaction
      , o.shop_spend_oneview
      , sf.webform_submitted
      , IF(sf.is_group_tickets_lead > 0, TRUE, FALSE) AS is_group_tickets_lead_oneview
      , IF(sf.is_fenway_premium_suites_lead > 0, TRUE, FALSE) AS is_fenway_premium_suites_lead_oneview
      , IF(sf.is_new_season_ticket_inquiries_lead > 0, TRUE, FALSE) AS is_new_season_ticket_inquiries_lead_oneview
      , IF(sf.is_mlb_brochure_download_lead > 0, TRUE, FALSE) AS is_mlb_brochure_download_lead_oneview
      , co.total_rfm_score_oneview
      , co.avidity_oneview
      , IFNULL(total_ticket_2017, 0) AS tickets_purchased_2017_oneview
      , IFNULL(total_ticket_2018, 0) AS tickets_purchased_2018_oneview
      , IFNULL(total_ticket_2019, 0) AS tickets_purchased_2019_oneview
      , IFNULL(total_ticket_2020, 0) AS tickets_purchased_2020_oneview

FROM    cte_oneview AS co
        LEFT OUTER JOIN cte_oneview AS o
              ON co.email = o.email
        LEFT OUTER JOIN cte_submitted_form AS sf
              ON co.email = sf.email
        LEFT OUTER JOIN cte_2017_oneview o7
            ON co.email = o7.email
        LEFT OUTER JOIN cte_2018_oneview o8
            ON co.email = o8.email
        LEFT OUTER JOIN cte_2019_oneview o9
            ON co.email = o9.email
        LEFT OUTER JOIN cte_2020_oneview o0
            ON co.email = o0.email