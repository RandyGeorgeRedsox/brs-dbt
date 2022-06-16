{{
    config(
        cluster_by=['account_id']
    )
}}

WITH account_calculated_fields AS (

  SELECT  a.account_id
        , COUNT(CASE WHEN SUBSTR(p.package_code, 1, 3) IN ('HSP','JBP','PRM','REG','SXP','TGW','TMP', 'PSE')
                AND SUBSTR(p.package_code, 4, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'current')
               THEN 1 END) AS current_red_sox_season_ticket_holder 
   
        , COUNT(CASE WHEN SUBSTR(p.package_code, 1, 3) IN ('HSP' ,'JBP' ,'PRM' ,'REG' ,'SXP' ,'TGW' ,'TMP','PSE')
                AND SUBSTR(p.package_code, 4, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'previous')
               THEN 1 END) AS lapsed_red_sox_season_ticket_holder 
   
        ,  COUNT(CASE WHEN pt.trait_id IN (1111, 1112, 1141, 1142) 
               THEN 1 END) AS red_sox_season_ticket_waitlist_member 
   
        ,  COUNT(CASE WHEN (e.event_code LIKE '19FE%' OR e.event_code LIKE '19RS%' OR e.event_code LIKE '19LC%' 
                       )
               THEN 1 END) AS individual_ticket_buyer_2019

        ,   COUNT(CASE WHEN (e.event_code LIKE '20FE%' OR e.event_code LIKE '20RS%' OR e.event_code LIKE '20LC%'
                       )
               THEN 1 END) AS individual_ticket_buyer_2020 

        ,   COUNT(CASE WHEN (e.event_code LIKE '21FE%' OR e.event_code LIKE '21RS%' OR e.event_code LIKE '21LC%' OR e.event_code LIKE '21SD%' OR e.event_code LIKE '21PS%')
               THEN 1 END) AS individual_ticket_buyer_2021

        ,   COUNT(CASE WHEN (e.event_code LIKE '22FE%' OR e.event_code LIKE '22RS%' OR e.event_code LIKE '22LC%' OR e.event_code LIKE '22SD%' OR e.event_code LIKE '22PS%')
               THEN 1 END) AS individual_ticket_buyer_2022
   
        , COUNT(CASE WHEN SUBSTR(p.package_code, 1, 3) IN ('HSP','SXP','TGW','TMP','PSE')
                AND SUBSTR(p.package_code, 4, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'current')
               THEN 1 END) AS current_red_sox_parital_plan_buyer 
   
        , COUNT(CASE WHEN SUBSTR(p.package_code, 1, 3) IN ('HSP','SXP','TGW','TMP','PSE')
                AND SUBSTR(p.package_code, 4, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'previous')
               THEN 1 END) AS lapsed_red_sox_partial_plan_buyer 
   
        , COUNT(CASE WHEN SUBSTR(e.event_code, 3, 2) = 'RS'
                AND SUBSTR(p.package_code, 1, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'current') 
               THEN 1 END) AS current_red_sox_group_buyer 
   
        , COUNT(CASE WHEN  SUBSTR(e.event_code, 3, 2) = 'RS'
                AND SUBSTR(p.package_code, 1, 2) IN (SELECT CAST(year AS STRING) FROM {{ ref('season_year') }} WHERE season = 'previous')
               THEN 1 END) AS lapsed_red_sox_group_buyer 
                
        , COUNT(CASE WHEN  a.account_id IN (SELECT attending_patron_account_id FROM {{ ref('attending_patron') }} )
               THEN 1 END) AS is_attending_patron
               
   FROM   {{ ref('tdc__patron_account__base') }}  AS a
          LEFT OUTER JOIN   {{ ref('tdc__patron_order__base') }}  AS po
              ON po.financial_account_id = a.account_id
          LEFT OUTER JOIN   {{ ref('tdc__order_line_item__base') }}  AS ol
              ON ol.order_id = po.order_id
          LEFT OUTER JOIN   {{ ref('tdc__event__base') }}  AS e 
              ON e.event_id = ol.event_id
          LEFT OUTER JOIN   {{ ref('tdc__package__base') }}  AS p 
              ON p.package_id = ol.package_id
          LEFT OUTER JOIN   {{ ref('tdc__patron_trait__base') }}  pt
              ON pt.account_id = a.account_id
      
   GROUP BY a.account_id
),

e AS(
    SELECT  SUBSTR(event_code, 1, 4) AS event_code
          , event_id
    FROM    {{ ref('tdc__event__base') }}
    WHERE SUBSTR(event_code, 3, 2) = 'RS' AND CAST(SUBSTR(event_code, 1, 2) AS int64) >= 13
),
      
t AS (
    SELECT  order_id
          , order_line_item_id
          , buyer_type_id
          , price AS ticket_price
    FROM    {{ ref('tdc__ticket__base') }} 
    WHERE price != 0 
),

p AS (
    SELECT DISTINCT 
            package_code
          , package_id 
    FROM    {{ ref('tdc__package__base') }}
    WHERE package_code NOT LIKE 'FAP%' 
      AND package_code NOT LIKE '%LV%' 
      AND package_code NOT LIKE 'FP%'
      AND package_code NOT LIKE 'FSP%' 
      AND package_code NOT LIKE 'JP%' 
      AND package_code NOT LIKE 'SBP%' 
),

account_patron_order AS (
    SELECT DISTINCT 
          p1.financial_account_id
        , p1.provenue_sales_rep
        , service_rep_id
        , p1.order_id
        , p1.order_date
        , order_line_item_id
        , ol.package_id
        , ol.event_id
   FROM   {{ ref('tdc__patron_order__base') }} AS p1
   
          INNER JOIN   {{ ref('tdc__order_line_item__base') }} AS ol
            ON ol.order_id = p1.order_id
   
   WHERE ol.market_type_code = 'P' 
   AND transaction_type_code IN ('SA', 'CS', 'ES') 

),

b1 AS (
    SELECT  bt.buyer_type_id
          , bg.buyer_type_group_code 
    FROM    {{ ref('tdc__buyer_type__base') }} AS bt
            LEFT OUTER JOIN  {{ ref('tdc__buyer_type_bt_grp__base') }} tg
               ON tg.buyer_type_id = bt.buyer_type_id
            INNER JOIN  {{ ref('tdc__buyer_type_group__base') }} AS bg
               ON bg.buyer_type_group_id = tg.buyer_type_group_id
               AND bg.description = 'All Boston Groups'

    UNION ALL

    SELECT  bt.buyer_type_id
          , bg.buyer_type_group_code  
    FROM    {{ ref('tdc__buyer_type__base') }} AS bt
            INNER JOIN  {{ ref('tdc__buyer_type_group__base') }} AS bg
               ON bg.buyer_type_group_id = bt.buyer_type_group
    WHERE bg.buyer_type_group_code = 'SINGLE' 
    ),

account_buyer_type AS (
    SELECT  buyer_type_id
          , MIN(buyer_type_group_code) AS buyer_type_group_code
    FROM    b1
    GROUP BY buyer_type_id

),

account_event_ticket_package AS (
    SELECT  financial_account_id 
          , ticket_price
          , po.order_id
          , p.package_id
          , po.event_id
          , t.buyer_type_id
          , p.package_code
          , bt.buyer_type_group_code
          , e.event_code
    FROM    account_patron_order po
            INNER JOIN e
             ON e.event_id = po.event_id
            INNER JOIN t
             ON po.order_line_item_id = t.order_line_item_id
            AND po.order_id = t.order_id
            LEFT OUTER JOIN  account_buyer_type bt
            ON bt.buyer_type_id = t.buyer_type_id
            LEFT OUTER JOIN p
            ON p.package_id = po.package_id
    
),

bb AS (
     SELECT  financial_account_id
           , ticket_price AS total_spend 
     
           , CASE WHEN SUBSTR(aa.event_code, 1, 2) = SUBSTR(CAST(CURRENT_DATE()AS string), 3, 2) 
                     AND package_code IS NOT NULL 
                     THEN ticket_price END as package_spend
           
           , CASE WHEN SUBSTR(aa.event_code, 1, 2) = SUBSTR(CAST(CURRENT_DATE()AS string), 3, 2) 
                     AND buyer_type_group_code = 'ALLGRP' 
                     THEN ticket_price END AS group_spend
            
           , CASE WHEN SUBSTR(aa.event_code, 1, 2) = SUBSTR(CAST(CURRENT_DATE()AS string), 3, 2) 
                     AND buyer_type_group_code = 'SINGLE' 
                     THEN ticket_price END AS single_spend
           
           , CASE WHEN SUBSTR(event_code, 1, 2) = SUBSTR(CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 year) AS string), 3, 2)
                     THEN ticket_price END AS last_year_spend
     
     FROM    account_event_ticket_package aa
),

account_spend AS (
    SELECT  financial_account_id
        , SUM(total_spend) as total_spend
        , SUM(package_spend) as package_spend
        , SUM(group_spend) as group_spend
        , SUM(single_spend) as single_spend
        , SUM(last_year_spend) as last_year_spend
    
    FROM    bb
    GROUP BY financial_account_id

),

e2 AS (
      SELECT  event_code
			, event_id 
      FROM    {{ ref('tdc__event__base') }}
      WHERE (event_code LIKE ('TPEL19%')) OR (event_code LIKE ('TPEL20%')) OR (event_code LIKE ('TPEL21%')) OR (event_code LIKE ('TPEL22%'))
),

p2 AS (
      SELECT  
              DISTINCT package_code
		    , package_id 
	  FROM    {{ ref('tdc__package__base') }}
      WHERE   package_code NOT LIKE 'FAP%' 
	  		AND package_code NOT LIKE '%LV%' 
	  		AND package_code NOT LIKE 'FP%'
              AND package_code NOT LIKE 'FSP%' 
	  		AND package_code NOT LIKE 'JP%' 
	  		AND package_code NOT LIKE 'SBP%' 
),

account_order_line_item_event AS (
      SELECT  ol.created_ts
            , financial_account_id
            , po.order_id
            , po.service_rep_id
            , p2.package_id
            , ol.event_id
            , p2.package_code
            , e2.event_code
      FROM    {{ ref('tdc__patron_order__base') }} po
              INNER JOIN {{ ref('tdc__order_line_item__base') }} ol
                 ON po.order_id = ol.order_id
              INNER JOIN {{ ref('tdc__event__base') }} e2
                 ON e2.event_id = ol.event_id
              LEFT OUTER JOIN p2
                 ON p2.package_id = ol.package_id
      WHERE financial_account_id IS NOT NULL 
          AND po.service_rep_id IS NOT NULL

),

b2 AS (
      SELECT  max(a.order_id) AS order_id
	  	      , a.financial_account_id
	    FROM    account_order_line_item_event a
	    GROUP BY 
	  		  a.financial_account_id

),

account_reps AS (
      SELECT  b2.order_id
            , b2.financial_account_id
            , por.service_rep_id
      FROM    b2
              INNER JOIN {{ ref('tdc__patron_order__base') }} por
                  ON por.order_id = b2.order_id
              INNER JOIN {{ ref('salesforce__users__latest') }} AS u
                  ON por.service_rep_id = u.provenue_user_id AND u.is_active IS TRUE

),
account_reserved AS (
    SELECT account_id
         , has_reserved_season_tickets
         , ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY has_reserved_season_tickets DESC) AS row_num
    FROM (
      SELECT  a.account_id
            , IF(COUNT(CASE WHEN transaction_type_code IN ('EV', 'RV') 
                            AND  SUBSTR(s.package_code, 4, 2) = '22'
                 THEN 1 END) 
                > 
                 COUNT(CASE WHEN transaction_type_code IN ('CR', 'EL', 'ER', 'RL', 'RT') 
                       AND  SUBSTR(s.package_code, 4, 2) = '22'
                 THEN 1 END), TRUE, FALSE) AS has_reserved_season_tickets

      FROM     {{ ref('tdc__patron_account__base') }}  AS a
               LEFT OUTER JOIN   {{ ref('tdc__patron_order__base') }}  AS po
                   ON a.account_id = po.financial_account_id 
               LEFT OUTER JOIN  {{ ref('tdc__order_line_item__base') }} oli
                   ON oli.order_id = po.order_id
               LEFT OUTER JOIN  {{ ref('season_ticket_packages') }}  AS s
                   ON oli.package_id = s.package_id 
      GROUP BY a.account_id

      UNION ALL

      SELECT  DISTINCT AccountId AS account_id
            , TRUE AS has_reserved_season_tickets
      FROM    {{ ref('2021_2022_sth_reservation') }}
    )
)

, set_active AS (
    SELECT DISTINCT email
         , set_active
    FROM (
      SELECT  DISTINCT na.email
            , 1 AS set_active

      FROM    {{ ref('provenue_contact') }} AS pc
              INNER JOIN  {{ ref('non_active_account') }} AS na
                  ON pc.email = na.email

      UNION ALL

      SELECT  DISTINCT pc.email
            , 1 AS set_active

      FROM    {{ ref('provenue_contact') }} AS pc
              LEFT OUTER JOIN UNNEST(csv_audiences) AS csv
      WHERE csv = 'Last Minute April May Buyers'

      UNION ALL

      
      SELECT  DISTINCT email
            , 1 AS set_active
      FROM    {{ ref('wheelhouse__paid_ads_lead_submissions__latest') }}

    )
)
, suite_renter AS (
      SELECT  DISTINCT po.financial_account_id
            , 1 AS red_sox_suite_renter
      FROM    {{ ref('tdc__patron_order__base') }} po
              LEFT OUTER JOIN {{ ref('tdc__ticket__base') }} t
                  ON po.order_id = t.order_id
              LEFT OUTER JOIN {{ ref('tdc__buyer_type__base') }} bt 
                  ON t.buyer_type_id = bt.buyer_type_id
      WHERE bt.buyer_type_code = 'PSSNPP'
      AND   price != 0
)

, merged_account AS (
      SELECT  DISTINCT Account_ID AS account_id
      FROM    {{ ref('merged_account') }}
)

, merged_target AS (
      SELECT  DISTINCT TARGET_PATRON_ACCOUNT_ID AS account_id
      FROM    {{ ref('merged_account') }}
)

SELECT  a.account_id
      , a.account_name
      , a.created_ts
      , 'ProVenue' AS account_source
      , a.account_type_code
      , pc.contact_id
      , pc.email 
      , pc.phone
      , pc.address
      , reps.service_rep_id 
      , IF(
            c.is_attending_patron > 0
            OR c.current_red_sox_season_ticket_holder > 0 
            OR c.lapsed_red_sox_season_ticket_holder > 0
            OR c.red_sox_season_ticket_waitlist_member > 0 
            OR c.individual_ticket_buyer_2019 > 0
            OR c.individual_ticket_buyer_2020 > 0 
            OR c.individual_ticket_buyer_2021 > 0
            OR c.individual_ticket_buyer_2022 > 0
            OR c.current_red_sox_parital_plan_buyer > 0
            OR c.lapsed_red_sox_partial_plan_buyer > 0 
            OR c.current_red_sox_group_buyer > 0 
            OR c.lapsed_red_sox_group_buyer > 0
            OR sa.set_active = 1 
            OR sr.red_sox_suite_renter = 1, 1, 0) AS is_active
     
      
    -- Calculated Fields --
      , IF(c.current_red_sox_season_ticket_holder > 0, 1, 0)  AS current_red_sox_season_ticket_holder 
      , IF(c.lapsed_red_sox_season_ticket_holder > 0, 1, 0)  AS lapsed_red_sox_season_ticket_holder 
      , IF(c.red_sox_season_ticket_waitlist_member > 0, 1, 0)  AS red_sox_season_ticket_waitlist_member 
      , IF(c.individual_ticket_buyer_2019 > 0, 1, 0)  AS individual_ticket_buyer_2019
      , IF(c.individual_ticket_buyer_2020 > 0, 1, 0)  AS individual_ticket_buyer_2020 
      , IF(c.individual_ticket_buyer_2021 > 0, 1, 0)  AS individual_ticket_buyer_2021
      , IF(c.individual_ticket_buyer_2022 > 0, 1, 0)  AS individual_ticket_buyer_2022
      , IF(c.current_red_sox_parital_plan_buyer > 0, 1, 0)  AS current_red_sox_parital_plan_buyer 
      , IF(c.lapsed_red_sox_partial_plan_buyer > 0, 1, 0) AS lapsed_red_sox_partial_plan_buyer 
      , IF(c.current_red_sox_group_buyer > 0, 1, 0)  AS current_red_sox_group_buyer 
      , IF(c.lapsed_red_sox_group_buyer > 0, 1, 0)  AS lapsed_red_sox_group_buyer 
      , red_sox_suite_renter
      , has_reserved_season_tickets
    
    -- Aggregate Fields --
      , s.total_spend
      , s.package_spend
      , s.group_spend
      , s.single_spend
      , s.last_year_spend
      , IF(ma.account_id IS NOT NULL, TRUE, FALSE) AS is_merged
      , IF(mt.account_id IS NOT NULL, TRUE, FALSE) AS is_merged_target
      , pe.plan_event_total_spend

FROM    {{ ref('tdc__patron_account__base') }} AS a
        
        LEFT OUTER JOIN {{ ref('provenue_contact') }} AS pc
            ON a.account_id = pc.account_id 
        
        LEFT OUTER JOIN set_active AS sa
            ON pc.email = sa.email
        
        LEFT OUTER JOIN merged_account AS ma
            ON a.account_id = ma.account_id

        LEFT OUTER JOIN merged_target AS mt
            ON a.account_id = mt.account_id

        LEFT OUTER JOIN account_calculated_fields AS c
            ON c.account_id = a.account_id
        
        LEFT OUTER JOIN account_spend AS s
            ON s.financial_account_id = a.account_id     
        
        LEFT OUTER JOIN account_reps AS reps
            ON reps.financial_account_id = a.account_id 
        
        LEFT OUTER JOIN account_reserved AS ar
            ON  pc.account_id = ar.account_id AND ar.row_num = 1
        
        LEFT OUTER JOIN suite_renter AS sr
            ON  pc.account_id = sr.financial_account_id

        LEFT OUTER JOIN {{ ref('plan_event_total_spend') }} AS pe
            ON  a.account_id = pe.pv_account_id

WHERE   pc.is_primary 
  AND   pc.primary_rank = 1


