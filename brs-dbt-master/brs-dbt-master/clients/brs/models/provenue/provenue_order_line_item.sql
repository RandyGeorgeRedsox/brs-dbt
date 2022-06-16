{{
    config(
        materialized='view'
    )
}}


SELECT    ol.order_line_item_id
        , ol.order_id
        , po.financial_account_id AS provenue_account_id
        , pac.description
        , ARRAY_AGG(
          STRUCT(
          e.event_date
        , e.event_name
        , es.price_scale
        , d.delivery_method
        , SUBSTR(e.event_code, 1, 2) AS event_season
        , s.seat_row
        , s.seat_number
        , s.section_id
        , t.status)
        ) AS event
        , ol.package_id
        , ol.transaction_type_code
        

FROM      {{ ref('tdc__order_line_item__base') }} AS ol

          LEFT OUTER JOIN   {{ ref('tdc__delivery__base') }} d
               ON d.transaction_id = ol.transaction_id
     
          LEFT OUTER JOIN   {{ ref('tdc__package__base') }} pac
               ON pac.package_id = ol.package_id
     
          LEFT OUTER JOIN   {{ ref('tdc__event_seat__base') }} es
               ON es.order_ID = ol.order_id
     
          LEFT OUTER JOIN   {{ ref('tdc__event__base') }} e
               ON e.event_id = es.event_id
     
          LEFT OUTER JOIN   {{ ref('tdc__ticket__base') }} t
               ON t.ticket_id = es.ticket_id
     
          LEFT OUTER JOIN   {{ ref('tdc__seat__base') }} s
               ON es.seat_id = s.seat_id

          LEFT OUTER JOIN   {{ ref('tdc__patron_order__base') }} po
               ON ol.order_id = po.order_id

GROUP BY  ol.order_line_item_id
        , ol.order_id
        , po.financial_account_id
        , pac.description
        , ol.package_id
        , ol.transaction_type_code
