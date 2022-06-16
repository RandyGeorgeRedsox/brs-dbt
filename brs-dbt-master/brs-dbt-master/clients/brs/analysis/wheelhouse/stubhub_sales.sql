{{
    config(
        materialized='view'
    )
}}

SELECT *
FROM (
  SELECT sh.ticket_id
        , sh.source_event_id
        , sh.event_name
        , e.game_pk
        , e.game_time_local
        , sh.confirmation_date
        , sh.sale_date
        , sh.refund_date
        , sh.seat_section
        , sh.seat_row
        , sh.seats
        , sh.seat_trait
        , sh.sale_quantity
        , sh.sale_amount
        , sh.seller_fee
        , sh.buyer_fee
        , sh.refund_quantity
        , sh.refund_amount
        , sh.buyer_stubhub_id
        , sh.seller_stubhub_id
        , sh.source_order_id
        , sh.stubhub_listing_id
        , row_number() over(partition by ticket_id order by batch_date desc) as dedupe
  FROM {{ ref('wheelhouse__stubhub_details_net__latest') }} sh
        INNER JOIN {{ ref('wheelhouse__baseball_game_master__latest') }} e ON sh.mlb_game_pk = e.game_pk
  WHERE e.game_time_local > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
) stubhub
WHERE dedupe = 1