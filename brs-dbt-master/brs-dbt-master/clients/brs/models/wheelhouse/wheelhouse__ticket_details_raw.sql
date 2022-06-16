{{
    config(
        materialized="table",
        cluster_by=['ticket_id']
    )
}}

SELECT  ticket_id
      , event_id
      , package_id
      , source_order_id
      , source_web_order_id
      , source_ticket_id
      , order_date
      , section_code
      , section_row_number
      , seat_number
      , quantity
      , sale_amount
      , refund_date
      , refund_amount
      , source_financial_patron_id
      , financial_patron_first_name
      , financial_patron_last_name
      , financial_patron_day_phone
      , financial_patron_evening_phone
      , financial_patron_mobile_phone
      , financial_patron_address_1
      , financial_patron_address_2
      , financial_patron_city
      , financial_patron_state
      , financial_patron_zip
      , financial_patron_email_id
      , financial_patron_email
      , omtr_partner_id
      , omtr_affiliate_id
      , omtr_vb_id
      , omtr_tdl_id
      , omtr_tfl_id
      , omtr_mlbkw_id
      , source_package_order_origin_desc
      , source_sale_type_desc
      , price_scale_desc
      , order_line_item_id
      , transaction_date
      , sales_channel_desc
      , sales_channel_type_desc
      , omtr_device_name
      , omtr_device_category
      , omtr_nskw_id
      , batch_date
      , team_nickname
      , team_id

FROM    {{ source('wheelhouse_red_sox', 'ticket_details_tdc') }} 