{{
config(
    unique_key = 'email',
    partition_by={
      "field": "created_dt",
      "data_type": "date"
    },
    cluster_by = ['user_id', 'created_dt'],
)
}}

SELECT
  email AS user_id
  , order_date AS created_dt
  , COUNT(ticket_id) AS value

FROM {{ ref('master_store_ticket_transaction') }}
WHERE refund_date IS NULL

-- {% if is_incremental() %}
--   AND order_date >= (SELECT MAX(order_date) FROM {{ this }} )
-- {% endif %}

GROUP BY email
  , order_date
