{{
    config(
        materialized= 'table',
        cluster_by=["pv_account_id"]
    )
}}

WITH stubhub_details AS (
    SELECT * 
    FROM (
    SELECT *
        , ROW_NUMBER() OVER(PARTITION BY buyer_email, seller_email, seat_section, seats, sale_amount, seat_row ORDER BY event_date DESC) AS row_num
    FROM {{ ref('wheelhouse__stubhub_details__latest') }}
    )
    WHERE row_num = 1
)


,   wheelhouse_transaction AS (
SELECT  t.source_financial_patron_id as pv_account_id
      , t.ticket_id
      , t.event_id
      , t.package_id
      , t.source_order_id
      , t.source_web_order_id
      , t.source_ticket_id
      , CAST(t.order_date AS DATE) AS order_date
      , t.section_code
      , t.section_row_number
      , NULL AS seat_section
      , t.seat_number
      , t.quantity
      , CAST(t.sale_amount AS FLOAT64) AS sale_amount
      , CAST(t.refund_date AS DATE) AS refund_date
      , CAST(t.refund_amount AS FLOAT64) AS refund_amount
      , t.financial_patron_first_name
      , t.financial_patron_last_name
      , t.financial_patron_day_phone
      , t.financial_patron_evening_phone
      , t.financial_patron_mobile_phone
      , t.financial_patron_address_1
      , t.financial_patron_address_2
      , t.financial_patron_city
      , UPPER(t.financial_patron_state) AS financial_patron_state
      , t.financial_patron_zip
      , t.financial_patron_email_id
      , t.financial_patron_email AS email
      , t.omtr_partner_id
      , t.omtr_affiliate_id
      , t.omtr_vb_id
      , t.omtr_tdl_id
      , t.omtr_tfl_id
      , t.omtr_mlbkw_id
      , t.source_package_order_origin_desc
      , t.source_sale_type_desc
      , t.price_scale_desc
      , t.order_line_item_id
      , CAST(t.transaction_date AS DATE) AS transaction_date
      , t.sales_channel_desc
      , t.sales_channel_type_desc
      , t.omtr_device_name
      , t.omtr_device_category
      , t.omtr_nskw_id
      , e.source_event_id
      , e.home_team
      , e.away_team
      , CAST(e.event_date AS DATE) AS event_date
      , e.event_desc
      , e.event_type_desc
      , e.mlb_game_pk
      , e.ignore_flag
      , CAST(e.dw_modified_date AS DATE) AS dw_modified_date
      , e.event_code
      , IF(e.event_date > CURRENT_DATE(), FALSE, u.is_unused_ticket) AS is_unused_ticket
      , 'primary' AS purchase_platform_type

FROM  {{ ref('wheelhouse__ticket_details__latest') }} AS t
      LEFT OUTER JOIN  {{ ref('wheelhouse__ticket_details_events_tdc__latest') }} AS e
        USING (event_id)
      LEFT OUTER JOIN {{ ref('unused_ticket') }} AS u
        ON t.ticket_id = u.ticket_id

UNION ALL

SELECT  NULL AS pv_account_id
      , n.ticket_id
      , NULL AS event_id
      , NULL AS package_id
      , n.source_order_id
      , NULL AS source_web_order_id
      , NULL AS source_ticket_id
      , CAST(n.sale_date AS DATE) AS order_date
      , NULL AS section_code
      , NULL AS section_row_number
      , n.seat_section
      , n.seats AS seat_number
      , n.sale_quantity AS quantity
      , CAST(n.sale_amount AS FLOAT64) AS sale_amount
      , CAST(n.refund_date AS DATE) AS refund_date
      , CAST(n.refund_amount AS FLOAT64) AS refund_amount
      , n.buyer_first_name AS financial_patron_first_name
      , n.buyer_last_name AS financial_patron_last_name
      , n.buyer_phone_1 AS financial_patron_day_phone
      , NULL AS financial_patron_evening_phone
      , n.buyer_phone_1 AS financial_patron_mobile_phone
      , n.buyer_address_1 AS financial_patron_address_1
      , n.buyer_address_2 AS financial_patron_address_2
      , n.buyer_address_city AS financial_patron_city
      , UPPER(n.buyer_address_state) AS financial_patron_state
      , n.buyer_address_zip AS financial_patron_zip
      , n.mlbam_email_id_buyer AS financial_patron_email_id
      , n.buyer_email AS email
      , NULL AS omtr_partner_id
      , NULL AS omtr_affiliate_id
      , NULL AS omtr_vb_id
      , NULL AS omtr_tdl_id
      , NULL AS omtr_tfl_id
      , NULL AS omtr_mlbkw_id
      , NULL AS source_package_order_origin_desc
      , NULL AS source_sale_type_desc
      , NULL AS price_scale_desc
      , NULL AS order_line_item_id
      , CAST(n.sale_date AS DATE) AS transaction_date
      , NULL AS sales_channel_desc
      , NULL AS sales_channel_type_desc
      , NULL AS omtr_device_name
      , NULL AS omtr_device_category
      , NULL AS omtr_nskw_id
      , CAST(n.source_event_id AS INT64) AS source_event_id
      , 'BOSTON RED SOX' AS home_team
      , tm.pv_away_team AS away_team
      , CAST(d.event_date AS DATE) AS event_date
      , n.event_name AS event_desc
      , n.event_type AS event_type_desc
      , n.mlb_game_pk
      , NULL AS ignore_flag
      , NULL AS dw_modified_date
      , NULL event_code
      , CAST(NULL AS BOOL) AS is_unused_ticket
      , 'secondary' AS purchase_platform_type

FROM    {{ ref('wheelhouse__stubhub_details_net__latest') }} AS n
        LEFT OUTER JOIN {{ ref('team_mapping') }} tm
            ON n.away_team = tm.stubhub_away_team
        LEFT OUTER JOIN stubhub_details AS d
            ON TIMESTAMP(n.confirmation_date) = d.confirmation_date 
            AND n.buyer_email = d.buyer_email 
            AND n.seller_email = d.seller_email
            AND n.seat_section = d.seat_section
            AND n.seats = d.seats
            AND n.sale_amount = d.sale_amount
            AND n.seat_row = d.seat_row

            
)

SELECT wt.*
      , mt.bill_to_contact 
      , mt.buyer_type_code 
      , mt.buyer_type_group_code 
      , mt.marketing_source_id  
      , mt.status
      , mt.sales_revenue_amount 
      , mt.order_type 
      , mt.patron_name 
      , mt.primary_contact 
      , mt.provenue_sales_rep  
      , mt.current_sales_balance 
      , mt.attending_account_id
      , mt.price_scale
      , mt.delivery_method
      , mt.total_package_quantity
      , mt.season_code 
      , mt.description 
      , mt.package_type 
      , mt.package_code
FROM  wheelhouse_transaction AS wt
      LEFT OUTER JOIN {{ ref('master_store_transaction') }} AS mt
        ON wt.source_order_id = mt.order_id