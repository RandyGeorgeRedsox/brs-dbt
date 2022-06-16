{{
    config(
        materialized="view"
    )
}}

WITH order_line_item_filter AS (
    SELECT  order_line_item_id
          , order_id
          , event_id
          , transaction_type_code
          , transaction_id
          , package_line_id
          , package_id
          , package_list_line_id --newly added
    FROM    {{ ref('tdc__order_line_item__base') }} 
    WHERE transaction_type_code IN ('CS', 'SA', 'RV') 
)
, ticket_section AS (
    SELECT  t.order_id
          , t.transaction_id
          , t.order_line_item_id
          , t.remove_order_line_item_id
          , SUM(t.price) AS price
          , t.price_scale_id
          , sc.public_description AS section_description --newly added
    FROM {{ ref('tdc__ticket__base') }} AS t
    INNER JOIN {{ ref('tdc__seat__base') }} AS s
        ON t.seat_id = s.seat_id
    INNER JOIN {{ ref('tdc__section__base') }} AS sc
        ON sc.section_id = s.section_id
    GROUP BY
          t.order_id
        , t.transaction_id
        , t.order_line_item_id
        , t.remove_order_line_item_id
        , t.price_scale_id
        , sc.public_description
)
, service_charge AS (
    SELECT  i.order_id
          , SUM(i.actual_amount) AS actual_amount
    FROM {{ ref('tdc__service_charge_item__base') }} AS i --need full-refresh
    INNER JOIN {{ ref('tdc__service_charge__base') }} AS c --need full-refresh
        ON c.service_charge_id = i.service_charge_id 
        AND c.public_description LIKE '%Royal%' 
        AND i.transaction_type_code IN ('CS', 'SA')
    GROUP BY 
        i.order_id
)
, package_list_line_seat AS (
    SELECT  plls.package_list_line_id 
          , plls.package_line_id 
          , plls.order_id
          , plls.current_transaction_type_code 
          , plls.reference_price_scale_id 
          , plls.reference_seat_id 
          , sc.public_description AS section_description
    FROM    {{ ref('tdc__package_list_line_seat__base') }} AS plls --need to add a new model
            INNER JOIN {{ ref('tdc__seat__base') }} s
                ON PLLS.REFERENCE_Seat_ID = s.seat_id
            INNER JOIN {{ ref('tdc__section__base') }} sc
                ON sc.section_id = s.section_id
    -- GROUP BY  
    --         plls.package_list_line_id 
    --       , plls.package_line_id 
    --       , plls.order_id
    --       , plls.current_transaction_type_code 
    --       , plls.reference_price_scale_id 
    --       , plls.reference_seat_id 
    --       , sc.public_description
)
, package_line AS (
    SELECT DISTINCT  pl.package_line_id
         , pl.package_id
         , pl.order_id
         , p.description
         , p.full_season_equiv --new filed adde
         , ps.description AS price_scale
         , ps.price_scale_code
         , plls.reference_price_scale_id
         , COUNT(DISTINCT plls.reference_seat_id) AS total_seats
         , plls.package_list_line_id
         , plls.section_description
           FROM {{ ref('tdc__package_line__base') }} pl
           INNER JOIN {{ ref('tdc__package__base') }} p
               ON p.package_id = pl.package_id
               AND (p.description LIKE '2021%'
                 OR p.description LIKE '2020%'
                 OR p.description LIKE '2019%'
                 OR p.description LIKE '2018%'
                   ) 
                AND p.description NOT LIKE '%Parking%' 
                AND p.description NOT LIKE '%Spring Training%'
           INNER JOIN  {{ ref('tdc__package_list_line__base') }} pll --need to add a new model
               ON pll.package_line_id = pl.package_line_id
               AND pll.package_id = pl.package_id
               AND pll.order_id = pl.order_id
               AND pll.transaction_id = pl.transaction_id
           INNER JOIN  package_list_line_seat plls
              ON plls.package_list_line_id = pll.package_list_line_id
              AND plls.package_line_id = pll.package_line_id
              AND plls.order_id = pll.order_id
              AND plls.current_transaction_type_code IN ('RV', 'SA', 'CS')
                
           LEFT JOIN {{ ref('tdc__price_scale__base') }} ps
               ON ps.price_scale_id = plls.reference_price_scale_id

           GROUP BY 
                  pl.package_line_id
                , pl.package_id
                , pl.order_id
                , p.description
                , p.full_season_equiv
                , ps.description
                , ps.price_scale_code
                , plls.reference_price_scale_id
                , plls.package_list_line_id
                , plls.section_description
)
, order_line_details AS (
    SELECT  sa.price
          , sa.section_description
          , IFNULL(sc.actual_amount,0) AS rrprice
          , ol.order_id
          , ol.event_id
          , ol.order_line_item_id
          , ol.transaction_type_code
          , pl.package_line_id
          , pl.description AS package_description
          , pl.package_id
          , ol.transaction_id
          , pl.price_scale AS price_scale_desc
          , full_season_equiv
          , pl.price_scale
          , pl.price_scale_code
          , total_seats
    FROM order_line_item_filter ol
         INNER JOIN ticket_section sa
              ON sa.order_line_item_id = ol.order_line_item_id                   
              AND sa.transaction_id = ol.transaction_id  
              AND sa.ORDER_ID = ol.order_id
              AND sa.remove_order_line_item_id IS NULL
         LEFT OUTER JOIN service_charge sc
             ON sc.order_id = ol.order_id 
         INNER JOIN package_line pl
              ON pl.package_id = ol.package_id
             AND pl.order_id = ol.order_id
             AND pl.reference_price_scale_id = sa.price_scale_id
             AND ol.package_list_line_id = pl.package_list_line_id
             AND pl.section_description = sa.section_description
) 

, patron_order AS (
    SELECT  financial_account_id
          , provenue_sales_rep AS sales_rep_id
          , order_id
          , account_type_code AS patron_account_type_code
          , pa.account_name AS patron_account_name
          , p1.created_ts AS created_date
    FROM    {{ ref('tdc__patron_order__base') }} p1
            INNER JOIN {{ ref('tdc__patron_account__base') }} pa
                ON pa.account_id = p1.financial_account_id
                AND pa.account_id != 1112111
)
, events AS (
    SELECT  event_id
          , event_code
          , event_date
          , event_name AS description               
        FROM {{ ref('tdc__event__base') }}
        WHERE event_name LIKE '%Plan%'
)
, sales_rep AS (
    SELECT  sales_rep_id
          , sales_rep_sub_group_id 
          , formatted_name                
    FROM    {{ ref('tdc__sales_rep__base') }} --need to add a new model
)

SELECT  sr.formatted_name
      , oli.transaction_type_code
      , oli.section_description
      , oli.order_id
      , oli.package_id
      , oli.package_line_id
      , po.patron_account_type_code
      , po.financial_account_id
      , SUBSTR(e.description,1, 4) AS season
      , e.description
      , rs.description AS sales_group
      , oli.price AS revenueNoRR
      , oli.package_description
      , IFNULL(rrprice, 0) AS rrrevenue
      , oli.price + IFNULL(rrprice,0) AS revenue
      , po.patron_account_name
      , po.created_date AS transaction_date
      , e.event_date
      , price_scale_desc
      , oli.price_scale_code
      , CASE WHEN SUBSTR(e.description,1, 4) = '2021' AND full_season_equiv IS NULL THEN
            (CASE WHEN oli.package_description LIKE '%Tenth%' THEN 10/81
                WHEN oli.package_description LIKE '%Twenty%' THEN 20/81
                WHEN oli.package_description LIKE '%TGW%' THEN 20/81
                WHEN oli.package_description LIKE '%Plan B' THEN 52/81
                WHEN oli.package_description LIKE '%Plan C' THEN 29/81
                WHEN oli.package_description LIKE '%Half%' THEN 40/81
                WHEN oli.package_description LIKE '%Plan S' THEN 15/81
                END)
            ELSE full_season_equiv END AS full_season_equiv
      , total_seats
      , IFNULL(CASE WHEN oli.transaction_type_code IN ('SA','CS') THEN total_seats END, 0) AS sold_seats
      , IFNULL(CASE WHEN oli.transaction_type_code IN ('RV') THEN total_seats END, 0) AS resv_seats
    FROM order_line_details oli
        INNER JOIN patron_order po
            ON po.order_id = oli.order_id 
        INNER JOIN events e
            ON e.event_id = oli.event_id         
            AND (event_code LIKE '%21%' or event_code LIKE 'RS21%' OR
                event_code LIKE '%20%' or event_code LIKE 'RS20%' OR
                event_code LIKE '%19%' or event_code LIKE 'RS19%' OR
                event_code LIKE '%18%' or event_code LIKE 'RS18%' OR
                event_code LIKE '%17%' or event_code LIKE 'RS17%')
        LEFT OUTER JOIN sales_rep sr
            ON sr.sales_rep_id = po.sales_rep_id 
        LEFT OUTER JOIN {{ ref('tdc__sales_rep_sub_group__base') }} rs       --need to add a new model           
            ON rs.sales_rep_sub_group_id = sr.sales_rep_sub_group_id  
    GROUP BY
        sr.formatted_name
      , oli.transaction_type_code
      , oli.section_description
      , po.patron_account_type_code
      , po.financial_account_id
      , e.description
      , rs.description
      , oli.price
      , OLI.RRPrice
      , oli.order_id
      , oli.package_id
      , oli.package_line_id
      , oli.package_description
      , po.patron_account_name
      , po.created_date
      , e.event_date
      , price_scale_desc
      , oli.price_scale_code
      , total_seats
      , full_season_equiv

        
      
      





      
