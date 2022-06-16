{{
    config(
        cluster_by=['order_id']
    )
}}

WITH cte AS (
    SELECT  order_id
          , rp.payments AS refund_payments

    FROM {{ ref('appetize_latest') }}
    LEFT OUTER JOIN UNNEST(refunds) AS rp
)

SELECT  DISTINCT
        app.order_id
      , app.total
      , app.order_date
      , app.device_order_time 
      , app.order_type 
      , app.vendor_name 
      , app.vendor_id
      , app.eventid AS event_id
      , CAST(app.discount AS FLOAT64) AS total_discount
      , CAST(app.fee AS FLOAT64) AS fees
      , CAST(app.tip AS FLOAT64) AS tips
      --, CAST(r.amount AS FLOAT64) AS total_refund
      --, CAST(app.total AS FLOAT64) AS total
      --, CAST(i.inclusive_tax_amount AS FLOAT64) AS inclusive_tax
      --, CAST(app.total AS FLOAT64) - CAST(i.inclusive_tax_amount AS FLOAT64) AS gross_sales
      --, CAST(app.total AS FLOAT64) - CAST(i.inclusive_tax_amount AS FLOAT64) - CAST(app.discount AS FLOAT64) - CAST(r.amount AS FLOAT64) AS net_sales
  
FROM    {{ ref('appetize_latest') }} AS app
        LEFT OUTER JOIN cte AS c
          ON app.order_id = c.order_id
        LEFT OUTER JOIN UNNEST( refund_payments ) AS r
        LEFT OUTER JOIN UNNEST(items) AS i
        -- LEFT OUTER JOIN UNNEST(payments) AS p
       
        