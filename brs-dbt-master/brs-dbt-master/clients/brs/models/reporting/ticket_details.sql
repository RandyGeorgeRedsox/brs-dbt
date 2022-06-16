{{
    config(
        materialized="view"
      , cluster_by=['ticket_id']
    )
}}

--Combining account and ticket_id
WITH order_line_item_latest AS (
  SELECT  pa.account_id
        , pa.account_name
        , t.ticket_id
        , oli.order_line_item_id
        , oli.order_id
        , oli.transaction_type_code
        , oli.transaction_id
        , p.package_code 
        , pl.package_order_origin_code
        , CASE WHEN oli.usage_event_id IN (SELECT e.event_id FROM {{ ref('reporting_events') }} AS e) THEN oli.usage_event_id 
               WHEN oli.event_id IN (SELECT e.event_id FROM {{ ref('reporting_events') }} AS e) THEN oli.event_id
          END AS event_id
  FROM    {{ ref('tdc__patron_order__base') }} AS po
            LEFT OUTER JOIN {{ ref('tdc__patron_account__base') }} AS pa
                ON po.financial_account_id = pa.account_id
            LEFT OUTER JOIN {{ ref('tdc__order_line_item__base') }}  AS oli
                ON po.order_id = oli.order_id
            INNER JOIN {{ ref('tdc__ticket__base') }} t
                ON t.order_line_item_id = oli.order_line_item_id
            LEFT OUTER JOIN {{ ref('tdc__package_line__base') }} pl
              ON oli.package_line_id = pl.package_line_id
            LEFT OUTER JOIN {{ ref('tdc__package__base') }} p
              ON pl.package_id = p.package_id
  WHERE oli.market_type_code = 'P' 
--   AND oli.transaction_type_code IN ('SA', 'ES', 'CS', 'RT', 'ER') -- commenting out to select all types of transactions
)

, delivery AS (
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT  *
              , ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY last_updated_ts DESC) AS row_num
        FROM    {{ ref('tdc__delivery__base') }}
)
    WHERE row_num = 1
)


--joining events and transaction (&agency & delivery) to account
, oli_tx_join AS (
  SELECT  oli.account_id
        , oli.account_name
        , oli.ticket_id
        , oli.order_line_item_id
        , oli.transaction_type_code
        , e.event_date
        , e.event_code
        , e.event_description
        , oli.package_code 
        , oli.package_order_origin_code
        , DATE(tx.transaction_date) AS transaction_date
        , dm.delivery_method_code
        , dm.description AS delivery_method_description
        , ag.agency_code
        , ag.agency_description
  FROM    order_line_item_latest AS oli
          INNER JOIN {{ ref('tdc__order_transaction__base') }} AS ot
              ON oli.order_id = ot.order_id AND oli.transaction_id = ot.transaction_id
          INNER JOIN {{ ref('tdc__transaction__base') }}  AS tx
              ON oli.transaction_id = tx.transaction_id 
          LEFT OUTER JOIN delivery AS d
              ON tx.transaction_id = d.transaction_id
          LEFT OUTER JOIN {{ ref('tdc__delivery_method__base') }} AS dm
              ON d.delivery_method = dm.delivery_method_id
          LEFT OUTER JOIN {{ ref('tdc__agency__base') }} AS ag
              ON tx.agency_id = ag.agency_id
          INNER JOIN {{ ref('reporting_events') }} AS e
              ON oli.event_id = e.event_id
  WHERE   IFNULL(ot.order_trxn_assoc_type_code,'X') <> 'SI'
)

--tickets related
, tickets_filtered AS (
  SELECT  t.ticket_id
        , t.order_line_item_id
        , t.remove_order_line_item_id
        , t.price
        , pc.price_scale_code
        , bt.buyer_type_group_code
        , bt.buyer_type_group_description
        , bt.buyer_type_code
        , bt.buyer_type_description
        , s.seat_number
        , s.seat_row
        , s.seat_section_code
        , s.center_x
        , s.center_y

  FROM    {{ ref('tdc__ticket__base') }} AS t
          LEFT JOIN {{ ref('buyer_type') }} AS bt        
              ON t.buyer_type_id = bt.buyer_type_id 
          LEFT OUTER JOIN {{ ref('tdc__price_scale__base') }} pc
              ON t.price_scale_id = pc.price_scale_id
          LEFT OUTER JOIN {{ ref('seating_area_mapping') }} AS s
              ON t.seat_id = s.seat_id
)

  SELECT  oli.account_id
        , oli.account_name
        , oli.ticket_id
        , oli.agency_code
        , oli.agency_description
        , t.buyer_type_group_code
        , t.buyer_type_group_description
        , t.buyer_type_code
        , t.buyer_type_description
        , oli.event_date
        , oli.event_code
        , oli.event_description
        , oli.transaction_date
        , t.price_scale_code
        , oli.package_code 
        , oli.package_order_origin_code
        , oli.delivery_method_code
        , oli.delivery_method_description
        , t.seat_number
        , t.seat_row
        , t.seat_section_code
        , t.center_x
        , t.center_y
        , oli.transaction_type_code
        , IF(t.remove_order_line_item_id IS NULL, 1, -1) AS tickets 
        , IF(t.remove_order_line_item_id IS NULL, t.price, -1 * t.price) AS revenue
  FROM    tickets_filtered AS t
          INNER JOIN oli_tx_join AS oli
              ON t.ticket_id = oli.ticket_id




