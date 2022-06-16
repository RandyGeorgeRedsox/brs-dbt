{{
    config(
        cluster_by=['pv_account_id'],
        materialized = 'table'
    )
}}

WITH events_filtered AS (
  SELECT event_id
       , event_date 
  FROM   {{ ref('tdc__event__base') }}
  WHERE  REGEXP_CONTAINS(event_code, '.*(RS|LF).*') 
    OR   event_code LIKE '21SD%'
    AND  event_code NOT LIKE 'RS%T%LFE'
    AND  event_code NOT LIKE 'RS%VE'

)

, transaction AS (
SELECT  DISTINCT ticket_id
      , t.order_id
      , t.order_line_item_id
      , price AS ticket_price
      , e.event_date 
FROM    {{ ref('tdc__ticket__base') }} AS t
        INNER JOIN   {{ ref('tdc__order_line_item__base') }} AS ol 
            ON t.order_line_item_id = ol.order_line_item_id
        INNER JOIN   events_filtered AS e
            ON ol.event_id = e.event_id
WHERE   price != 0 
AND     ol.market_type_code = 'P' 
AND     transaction_type_code IN ('SA', 'CS', 'ES') 
AND     t.status NOT IN ('N', 'R')
)

, season_spending AS (
SELECT  po.financial_account_id AS pv_account_id
      , EXTRACT(YEAR FROM event_date) AS season
      , MAX(ticket_price) AS ticket_spending
      
FROM    {{ ref('tdc__patron_order__base') }} po
        INNER JOIN transaction AS t
            ON po.order_id = t.order_id 
WHERE financial_account_id IS NOT NULL
GROUP BY  po.financial_account_id
        , event_date
)

, cte AS (
SELECT  pv_account_id
      , ticket_spending
      , season
      , ROW_NUMBER() OVER(PARTITION BY pv_account_id, season ORDER BY ticket_spending DESC) AS row_num
FROM season_spending
)

SELECT  pv_account_id 
      , SUM(ticket_spending) AS plan_event_total_spend
FROM cte
WHERE row_num = 1
GROUP BY 1