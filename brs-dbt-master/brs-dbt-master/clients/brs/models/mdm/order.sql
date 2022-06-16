{{
    config(
        cluster_by=['account_name']
    )
}}

SELECT  po.account_name 
      , po.bill_to_contact 
      , po.buyer_type_code 
      , po.buyer_type_group_code 
      , '0052h000000kNA6AAM' AS created_by 
      , po.marketing_source_id  
      , po.status 
      , po.sales_revenue_amount 
      , po.order_start_date 
      , po.order_type 
      , po.patron_id_number 
      , po.patron_name 
      , po.primary_contact 
      , po.provenue_order_id
      , po.provenue_sales_rep  
      , po.current_sales_balance 
      , po.attending_account_id
      , po.price_scale
      , po.delivery_method
      , po.row
      , po.section_code
      , po.total_package_quantity
      , po.seats
      , po.order_amount
      , po.event_date 
      , po.event_name 
      , po.season_code 
      , po.description 
      , po.package_type 
      , po.package_code


FROM    {{ ref('provenue_order') }} AS po
        