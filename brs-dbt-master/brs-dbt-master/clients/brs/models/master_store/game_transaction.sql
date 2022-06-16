{{
    config(
        materialized="table",
        unique_key= "pv_account_id",
        cluster_by=["pv_account_id"]
    )
}}
        

SELECT  po.financial_account_id AS pv_account_id
      , po.order_id
      , t.order_line_item_id 
      , DATE(po.created_ts) AS created_date
      , COUNT(t.ticket_id) AS num_tickets

FROM    {{ ref('tdc__patron_order__base') }} po
        INNER JOIN {{ ref('transaction') }} AS t
            ON po.order_id = t.order_id 
GROUP BY  po.financial_account_id
        , po.order_id  
        , po.created_ts
        , t.order_line_item_id


      
     

