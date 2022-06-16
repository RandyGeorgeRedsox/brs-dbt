{{
    config(
        materialized= 'ephemeral'
    )
}}

SELECT  email
      , initial_order_date
      , charge_value
      , refund_date
      , order_line_id
      , invoice_id 
      , CONCAT(order_line_id, invoice_id ) AS primary_key
      , mlbam_dw_row_modified_date
      , cancel_date

FROM    {{ source('wheelhouse_red_sox', 'mlbtv_sales_full') }}