{{
    config(
        materialized= 'ephemeral'
    )
}}
SELECT  mlbam_viewed_in_app
      , ticket_print_scan_id
      , ticket_print_detail_id
      , order_id
      , transaction_id
      , last_updated_date 
      , CAST(scanned_timestamp AS DATE) AS scanned_dt
      , ticket_barcode
      , ticket_id
      , ticket_event_id
      , seat_id
      , financial_patron_id
      , financial_patron_guid
      , attending_patron_id
      , attending_patron_guid
      , scan_area_code
      , scan_result_code
      , scan_result_description
      , scan_result_type_description
      , scan_media_type_description
      , scan_device_id
      , scan_operator
      , scan_device_code
      , scan_area_id
      , nth_day_valid_scan_for_ticket
      , nth_day_digital_scan_for_cust
      , financial_patron_guid_email
      , financial_patron_tdc_email
      , attending_patron_guid_email
      , attending_patron_tdc_email
      , attending_patron_tdc_email AS email
      , financial_patron_okta_id
      , attending_patron_okta_id
      , seat_layout_code
      , seat_row_number
      , seat_number
      , CAST(scan_reset_date AS DATE) AS scan_reset_date
      , scan_reset_type_description
      , hierarchy_event_type
      , event_type
      , game_pk
      , batch_date
      , team_nickname
      , team_id

FROM    {{ source('wheelhouse_red_sox', 'ticket_scan_tdc') }} 