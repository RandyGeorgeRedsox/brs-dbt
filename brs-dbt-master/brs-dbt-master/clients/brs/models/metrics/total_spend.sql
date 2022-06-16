{{
config(
    partition_by={
      "field": "created_dt",
      "data_type": "date"
    },
    cluster_by = ['user_id', 'created_dt'],
)
}}

WITH cte AS (
  -- shop_spend 
  SELECT
    email
    , order_date
    , SUM(demand_quantity * selling_unit_price) AS value
  FROM {{ ref('wheelhouse__shop_sales__latest') }}
  GROUP BY email
    , order_date

  UNION ALL
  -- mlbtv_spend 
  SELECT
    email
    , DATE(initial_order_date) AS order_date
    , SUM(charge_value) AS value
  FROM {{ ref('wheelhouse__mlbtv_sales_full__latest') }}
  WHERE refund_date IS NULL
  GROUP BY email
    , DATE(initial_order_date)

  UNION ALL
  -- ticket_spend
  SELECT
    email AS email
    , order_date AS order_date
    , SUM(sale_amount) AS value
  FROM {{ ref('master_store_ticket_transaction') }}
  WHERE refund_date IS NULL
  GROUP BY email
    , order_date
)

SELECT
  email AS user_id
  , order_date AS created_dt
  , SUM(value) AS value
FROM cte

-- {% if is_incremental() %}
--   WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }} )
-- {% endif %}

GROUP BY email
  , order_date
