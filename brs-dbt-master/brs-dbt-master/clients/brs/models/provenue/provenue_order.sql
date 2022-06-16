{{
    config(
        materialized='view'
    )
}}


WITH order_contact AS (
    SELECT  c.account_name AS account_id 
          , c.contact_id AS contact_id
    FROM    {{ ref('tdc__patron_contact__base') }} AS c 
    WHERE c.is_primary = TRUE
    AND c.account_name != 6562187
),
     patron_order AS (
     SELECT DISTINCT 
            p1.financial_account_id
          , p1.provenue_sales_rep
          , p1.order_id
          
          , ol.order_line_item_id
          , ol.package_id
          , ol.event_id
          , ol.transaction_type_code
          
          , pk.package_code
          , pk.description

     FROM   {{ ref('tdc__patron_order__base') }} AS p1

            INNER JOIN {{ ref('tdc__order_line_item__base') }} AS ol
                   ON ol.order_id = p1.order_id
     
            LEFT OUTER JOIN {{ ref('tdc__package__base') }} AS pk
                   ON ol.package_id = pk.package_id

     WHERE ol.market_type_code = 'P'
     AND transaction_type_code IN ('SA', 'CS', 'ES') 

      ),

     event_season AS (
     SELECT  event_id
           , event_date
           , event_code
           , ee.event_name AS description
           , ss.season_code
  
     FROM    {{ ref('tdc__event__base') }} AS ee
             INNER JOIN  {{ ref('tdc__season__base') }} AS ss
                    ON ss.season_id = ee.season_id
     WHERE SUBSTR(event_code, 1, 4) in ('20RS', '19RS', '21SD', '21PS', '22RS') 

      ),

     transaction AS (
      SELECT t1.buyer_type_id
           , t1.order_line_item_id
           , t1.price AS ticket_price
           , 1 AS seat_count
           , st.seat_number
           
           , sc.section_code
           , st.seat_row AS row 
           , sc.description AS seat_location
           , ps.description AS price_scale
           
           , dm.description AS delivery_method
           
           , t1.order_id
           , t1.transaction_id
           , t1.ticket_id
           
      FROM   {{ ref('tdc__ticket__base') }} t1
             INNER JOIN {{ ref('tdc__transaction__base') }} t2
               ON t2.transaction_id = t1.transaction_id
             INNER JOIN {{ ref('tdc__seat__base') }} st
               ON st.seat_id = t1.seat_id
             INNER JOIN {{ ref('tdc__section__base') }} sc
               ON sc.section_id = st.section_id
             LEFT OUTER JOIN {{ ref('tdc__price_scale__base') }} ps
               ON ps.price_scale_id = t1.price_scale_id
             LEFT OUTER JOIN {{ ref('tdc__delivery__base') }} dd 
               ON dd.transaction_id = t2.transaction_id
             LEFT OUTER JOIN {{ ref('tdc__delivery_method__base') }} dm 
               ON dm.delivery_method_id = dd.delivery_method
      WHERE t1.price != 0

      ),

      buyer_type AS (
      SELECT DISTINCT 
            bt.buyer_type_id
          , bg.buyer_type_group_code
          , bt.buyer_type_code 
          , bg.description 
       
       FROM {{ ref('tdc__buyer_type__base') }} AS bt

            INNER JOIN {{ ref('tdc__buyer_type_group__base') }} AS bg
                   ON bg.buyer_type_group_id = bt.buyer_type_group

       WHERE bg.description = 'All Boston Groups' OR bg.buyer_type_group_code = 'SINGLE'
          OR bt.buyer_type_code IN ('XPX', 'XTF', 'XQF')
      ),

      patron_event_transaction AS (
      SELECT DISTINCT 
             financial_account_id
           , provenue_sales_rep
           , po.order_id
           , season_code
           , po.package_code 
           , po.description AS package_type
         
           , SUBSTR(event_code, 1, 4) AS event_code
           , CAST(event_date AS STRING) AS event_date
           , e.description AS event_name 
           , price_scale
           , delivery_method
           , transaction_id
         
           , row
           , section_code
           , seat_location
           , seat_number 
           , buyer_type_code
           , buyer_type_group_code
           , transaction_type_code
         
           , IF(buyer_type_group_code IN ('ALLGRP', 'SINGLE'), 0, 1) AS package_quantity
         
           , seat_count AS seat_quantity
         
           , ticket_price AS ticket_amount
           
        FROM patron_order AS po

             INNER JOIN event_season AS e
                 ON e.event_id = po.event_id   
             INNER JOIN transaction AS t
                 ON t.order_line_item_id = po.order_line_item_id
                 AND t.order_id = po.order_id   
             LEFT OUTER JOIN buyer_type AS bt
                 ON bt.buyer_type_id = t.buyer_type_id


      ),

      pre_orders AS (

      SELECT  financial_account_id
            , provenue_sales_rep 
            , order_id
            , season_code
            , event_code 
            , event_date 
            , CASE WHEN transaction_type_code IN ('CR', 'EL', 'ER', 'RL', 'RT') THEN 'Cancelled'
                   WHEN transaction_type_code IN ('EV', 'RV') THEN 'Reserved'
                   WHEN transaction_type_code IN ('SA', 'ES', 'CS') THEN 'Sold'
              END AS status 
            , buyer_type_code 
            , price_scale 
            , section_code 
            , row 
            , event_name 
            , delivery_method
            , package_code 
            , package_type 
            , buyer_type_group_code
            
            , CONCAT(IFNULL(buyer_type_code, 'No Buyer Type'), ' | ', price_scale,  ' | ', SUM(seat_quantity), ' | ', section_code, ' | ', row, ' | ', ARRAY_TO_STRING(ARRAY_AGG(seat_number), ',')  ,' | ', event_name, ' | ', delivery_method) AS description 
           
            , SUM(seat_quantity) AS seat_quantity 
           
            , SUM(package_quantity) as package_quantity
          
            , SUM(ticket_amount) as order_amount
            
      FROM    patron_event_transaction 

      GROUP BY 
                financial_account_id
              , provenue_sales_rep
              , order_id
              , season_code
              , event_code
              , event_name 
              , delivery_method 
              , price_scale 
              , row 
              , section_code 
              , buyer_type_code 
              , transaction_type_code 
              , event_date 
              , package_type
              , package_code
              , buyer_type_group_code
           ),
     
      order_final_agg AS (
       SELECT  financial_account_id 
             , provenue_sales_rep
             , order_id
             , season_code
             , event_code
             , status
             , ARRAY_AGG(DISTINCT IFNULL(package_code, '')) AS package_code
             , MAX(buyer_type_group_code) AS buyer_type_group_code
             , MAX(package_type) AS package_type
             , MAX(buyer_type_code) AS buyer_type_code
             , MAX(delivery_method) AS delivery_method
             , MAX(row) AS row
             , MAX(section_code) AS section_code
             , ARRAY_AGG(DISTINCT price_scale) AS price_scale
             , ARRAY_AGG(description) AS description
             , ARRAY_AGG(DISTINCT event_date) AS event_date 
             , ARRAY_AGG(DISTINCT event_name) AS event_name 
             , SUM(seat_quantity) AS seat_quantity
             , SUM(package_quantity) AS package_quantity 
             , SUM(order_amount) AS order_amount
       FROM    pre_orders
       WHERE description IS NOT NULL
       GROUP BY  financial_account_id
               , provenue_sales_rep
               , order_id
               , season_code
               , event_code
               , status
       ORDER BY order_id
             )

SELECT  po.financial_account_id AS account_name 
      , c.contact_id AS bill_to_contact 
      , o.buyer_type_code 
      , o.buyer_type_group_code 
      , '0052h000000kNA6AAM' AS created_by 
      , po.marketing_source_id  
      , status
      , po.sales_revenue_amount 
      , po.order_date AS order_start_date 
      , po.order_contents_type_code AS order_type 
      , po.financial_account_id AS patron_id_number 
      , po.patron_name 
      , c.contact_id AS primary_contact 
      , po.order_id AS provenue_order_id
      , o.provenue_sales_rep  
      , po.current_sales_balance 
      , po.attending_account_id
      , IF(ARRAY_LENGTH(o.price_scale) > 1, 'Multiple', ARRAY_TO_STRING(o.price_scale, '')) AS price_scale
      , o.delivery_method
      , o.row
      , o.section_code
      , o.package_quantity AS total_package_quantity
      , o.seat_quantity AS seats
      , CAST(o.order_amount AS INT64) AS order_amount
      , o.event_date 
      , o.event_name 
      , o.season_code 
      , o.description 
      , o.package_type 
      , o.package_code
      


FROM   {{ ref('tdc__patron_order__base') }} AS po
       LEFT OUTER JOIN   order_contact AS c 
           ON c.account_id = po.financial_account_id
       INNER JOIN   order_final_agg AS o
         ON o.financial_account_id = po.financial_account_id 
         AND o.order_id = po.order_id