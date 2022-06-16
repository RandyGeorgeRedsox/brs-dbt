{{
    config(
        materialized="table",
        unique_key= "ticket_id",
        cluster_by=["ticket_id"]
    )
}}
        
WITH events_filtered AS (
  SELECT event_id
  FROM   {{ ref('tdc__event__base') }}
  WHERE  REGEXP_CONTAINS(event_code, '.*(RS|LF).*') 
    OR   event_code LIKE '21SD%'
    OR   event_code LIKE '21PS%'
    AND  event_code NOT LIKE 'RS%T%LFE'
    AND  event_code NOT LIKE 'RS%VE'

)


SELECT  DISTINCT ticket_id
      , t.order_id
      , t.order_line_item_id
      , price AS ticket_price
FROM    {{ ref('tdc__ticket__base') }} t
        INNER JOIN   {{ ref('tdc__order_line_item__base') }} AS ol 
            ON t.order_line_item_id = ol.order_line_item_id
        INNER JOIN   events_filtered AS e
            ON ol.event_id = e.event_id
WHERE   price != 0 
AND     ol.market_type_code = 'P' 
AND     transaction_type_code IN ('SA', 'CS', 'ES') 
AND     t.status NOT IN ('N', 'R')

