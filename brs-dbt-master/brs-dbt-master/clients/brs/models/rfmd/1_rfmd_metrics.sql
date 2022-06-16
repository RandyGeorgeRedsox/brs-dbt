{{
    config(
        materialized="view",
        unique_key="pv_account_id",
        cluster_by=["pv_account_id"]
    )
}}

WITH orders_cte AS (
    SELECT  order_id
          , pv_account_id
          , DATE(created_date) AS order_date
          , ticket_spending
          , num_tickets

    FROM    {{ ref('master_store_transaction') }}
    WHERE   num_tickets > 0
            AND DATE(created_date) > DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR)
),

customers_cte AS (
    SELECT  pv_account_id
          , pv_account_created_date AS acquisition_date
          , DATE_DIFF(CURRENT_DATE, pv_account_created_date, YEAR) AS age_in_years
    FROM    {{ ref('master_store_contact') }}
    WHERE   is_designated_likely_broker IS FALSE 
      AND   is_designated_sponsor IS FALSE 
      AND   is_designated_mlb IS FALSE 
      AND   is_designated_trade IS FALSE 
      AND   is_designated_internal IS FALSE 
),

metrics_cte AS (
    SELECT  o.pv_account_id

            -- number of days since last order
          , DATE_DIFF(CURRENT_DATE, MAX(o.order_date), DAY) AS recency_metric

            -- number of lifetime orders
          , COUNT(o.order_date) AS lifetime_orders

            -- total spend, not bounded by a timeframe
          , SUM(o.ticket_spending) AS monetary_metric

            -- number of years/days since customer's account was created
          , CASE 
                  WHEN MIN(c.age_in_years) = 0 THEN 1 
                  WHEN MIN(c.age_in_years) > 5 THEN 5
          ELSE MIN(c.age_in_years) END AS age_in_years
          , DATE_DIFF(CURRENT_DATE, MIN(c.acquisition_date), DAY) AS age_metric

          , MIN(o.order_date) AS first_order_date

    FROM    orders_cte AS o
    INNER JOIN customers_cte AS c
        ON o.pv_account_id = c.pv_account_id

    GROUP BY 
            o.pv_account_id
)

SELECT  pv_account_id
      , recency_metric
      , CAST(lifetime_orders / age_in_years AS INT64) AS frequency_metric
      , monetary_metric
      , age_metric

      , lifetime_orders
      , first_order_date

FROM    metrics_cte