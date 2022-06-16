WITH order_transaction_latest AS (
  SELECT  transaction_id
        , order_id
        , order_trxn_assoc_type_code
          FROM  {{ ref('tdc__order_transaction__base') }}
  WHERE   IFNULL(order_trxn_assoc_type_code,'X') <> 'SI'
)

, events_latest_filtered AS (
  SELECT  event_id
        , event_date
        , event_code 
  FROM    {{ ref('tdc__event__base') }}
  WHERE   EXTRACT(YEAR FROM event_date) >= (SELECT MIN(year) FROM {{ ref('reference_years') }})
          AND (REGEXP_CONTAINS(event_code, '.*(RS|SD|LF|CA|GM|LC).*') --LC, live concert
               OR event_code LIKE 'RS%T%LFE')
          AND event_code NOT LIKE 'RS%VE'
       
)

, order_line_item_latest AS (
  SELECT  oli.order_line_item_id
        , order_id
        , transaction_id
        , CASE WHEN oli.usage_event_id IN (SELECT e.event_id FROM events_latest_filtered AS e) THEN oli.usage_event_id 
               WHEN oli.event_id IN (SELECT e.event_id FROM events_latest_filtered AS e) THEN oli.event_id
          END AS event_id
        , CAST(oli.transaction_type_code IN ('SA','ES','CS') AS INT64) AS sale
        , CAST(oli.transaction_type_code IN ('RT','ER') AS INT64) AS return
  FROM {{ ref('tdc__order_line_item__base') }}  AS oli
  WHERE oli.market_type_code = 'P' AND oli.transaction_type_code IN ('SA', 'ES', 'CS', 'RT', 'ER')
)

-- Joins and filters
, service_charge_items AS (
  SELECT  sci.ticket_id
        , SUM(sci.actual_amount) AS actual_amount
  FROM    {{ ref('tdc__service_charge_item__base') }} AS sci
          INNER JOIN {{ ref('tdc__service_charge__base') }} AS sc
              ON sci.service_charge_id = sc.service_charge_id
              AND sc.include_in_ticket_price = 1
  GROUP BY
          sci.ticket_id
)

, oli_tx_join AS (
  SELECT  oli.order_line_item_id
        , e.event_date
        , e.event_code
        , DATE(tx.transaction_date) AS transaction_date
        , e.event_id
        , sale
        , return
  FROM    order_line_item_latest AS oli
          INNER JOIN order_transaction_latest AS ot
              ON oli.order_id = ot.order_id AND oli.transaction_id = ot.transaction_id
          INNER JOIN {{ ref('tdc__transaction__base') }}  AS tx
              ON oli.transaction_id = tx.transaction_id 
          INNER JOIN events_latest_filtered AS e
              ON oli.event_id = e.event_id
)

, tickets_filtered AS (
  SELECT  t.order_line_item_id
        , t.remove_order_line_item_id
        , t.ticket_id
        , t.price
        , sci.actual_amount
        , bg.buyer_type_group_code
  FROM    {{ ref('tdc__ticket__base') }} AS t
          LEFT JOIN {{ ref('tdc__buyer_type__base') }} AS bt        
              ON bt.buyer_type_id = t.buyer_type_id   
          LEFT JOIN {{ ref('tdc__buyer_type_group__base') }}  AS bg       
              ON bg.buyer_type_group_id = bt.report_buyer_type_group_id  
          LEFT JOIN service_charge_items AS sci
              ON sci.ticket_id = t.ticket_id
)

-- Order line item joins and aggregations
, oli_join AS (
  SELECT  t.buyer_type_group_code
        , oli.transaction_date
        , oli.event_date
        , oli.event_code

        , COALESCE(SUM(sale), 0) AS sale_qty
        , COALESCE(SUM(return), 0) AS return_qty
        , COALESCE(SUM(sale * t.price), 0) AS sale_amount
        , COALESCE(SUM(return * t.price), 0) AS return_amount
        , COALESCE(SUM(sale * t.actual_amount), 0) AS sc_sale_amount
        , COALESCE(SUM(return * t.actual_amount), 0) AS sc_return_amount
        , COUNT(DISTINCT CASE WHEN t.price = 0 AND sale = 1 THEN ticket_id END) AS comp_sale_qty
        , COUNT(DISTINCT CASE WHEN t.price = 0 AND return = 1 THEN ticket_id END) AS comp_return_qty
  FROM    tickets_filtered AS t
          INNER JOIN oli_tx_join AS oli
              ON t.order_line_item_id = oli.order_line_item_id
  GROUP BY 
          t.buyer_type_group_code
        , oli.transaction_date
        , oli.event_date
        , oli.event_code
)

, remove_oli_join AS (
  SELECT  t.buyer_type_group_code
        , oli.transaction_date
        , oli.event_date
        , oli.event_code
        , COALESCE(SUM(sale), 0) AS sale_qty
        , COALESCE(SUM(return), 0) AS return_qty
        , COALESCE(SUM(sale * t.price), 0) AS sale_amount
        , COALESCE(SUM(return * t.price), 0) AS return_amount
        , COALESCE(SUM(sale * t.actual_amount), 0) AS sc_sale_amount
        , COALESCE(SUM(return * t.actual_amount), 0) AS sc_return_amount
        , COUNT(DISTINCT CASE WHEN t.price = 0 AND sale = 1 THEN ticket_id END) AS comp_sale_qty
        , COUNT(DISTINCT CASE WHEN t.price = 0 AND return = 1 THEN ticket_id END) AS comp_return_qty
  FROM    tickets_filtered AS t
          INNER JOIN oli_tx_join AS oli
              ON t.remove_order_line_item_id = oli.order_line_item_id
  GROUP BY 
          t.buyer_type_group_code
        , oli.transaction_date
        , oli.event_date
        , oli.event_code
)

, ticket_agg AS (
  SELECT * FROM oli_join
  UNION ALL
  SELECT * FROM remove_oli_join
)

-- Final summation of group quantities
, group_agg AS (
  SELECT  buyer_type_group_code
        , transaction_date
        , event_date
        , event_code
        , SUM(sale_qty) AS Sale_Qty
        , SUM(return_qty) AS Return_Qty
        , SUM(sale_amount) AS Sale_Amount
        , SUM(return_amount) AS Return_Amount
        , SUM(sc_sale_amount) AS SC_Sale_Amount
        , SUM(sc_return_amount) AS SC_Return_Amount
        , SUM(comp_sale_qty) AS comp_sale_qty
        , SUM(comp_return_qty) AS comp_return_qty
  FROM    ticket_agg AS t
  GROUP BY
          event_date
        , event_code 
        , transaction_date
        , buyer_type_group_code
)

-- Report output
SELECT  event_date
      , event_code
      , (SELECT year FROM {{ ref('reference_years') }} WHERE season='previous') AS prior_year
      , (SELECT year FROM {{ ref('reference_years') }} WHERE season='current') AS current_year
      , transaction_date
      , buyer_type_group_code

      -- Ticket sale count
      , sale_qty - return_qty +
        CASE WHEN event_code = '19RS0807' 
                  AND buyer_type_group_code = 'SNGFUL' 
                  AND transaction_date = '2019-08-07'
             THEN 9125 ELSE 0 END AS tickets

      -- Ticket sale revenue sum
      , CAST(sale_amount - return_amount - ( sc_sale_amount - sc_return_amount ) AS FLOAT64) AS revenue
      
      -- Added comp return qty per 8/14 request
      , comp_sale_qty
      , comp_return_qty
      , comp_sale_qty - comp_return_qty AS comp_tickets
      , sale_qty - return_qty - (comp_sale_qty - comp_return_qty) AS noncomp_tickets
FROM    group_agg