{{
    config(
        materialized="table",
        unique_key= "pv_account_id",
        cluster_by=["pv_account_id"]
    )
}}

WITH cte AS (
SELECT  po.financial_account_id AS pv_account_id
      , po.order_id
      , DATE(po.created_ts) AS created_date
      , EXTRACT(WEEK FROM DATE(po.created_ts)) AS week_of_year
      , SUM(t.ticket_price) AS ticket_spending
      , COUNT(DISTINCT t.order_line_item_id) AS num_games
      , COUNT(t.ticket_id) AS num_tickets
      
FROM    {{ ref('tdc__patron_order__base') }} po
        INNER JOIN {{ ref('transaction') }} AS t
            ON po.order_id = t.order_id 
GROUP BY  po.financial_account_id
        , po.order_id  
        , po.created_ts    
)  
        
SELECT  c.*
      , o.bill_to_contact 
      , o.buyer_type_code 
      , o.buyer_type_group_code 
      , o.marketing_source_id  
      , o.status
      , o.sales_revenue_amount 
      , o.order_type 
      , o.patron_name 
      , o.primary_contact 
      , o.provenue_sales_rep  
      , o.current_sales_balance 
      , o.attending_account_id
      , o.price_scale
      , o.delivery_method
      , o.row
      , o.section_code
      , o.total_package_quantity
      , o.seats
      , o.event_date 
      , o.event_name 
      , o.season_code 
      , o.description 
      , o.package_type 
      , o.package_code

FROM cte c
     LEFT OUTER JOIN {{ ref('provenue_order') }} AS o
            ON c.order_id = o.provenue_order_id

      
