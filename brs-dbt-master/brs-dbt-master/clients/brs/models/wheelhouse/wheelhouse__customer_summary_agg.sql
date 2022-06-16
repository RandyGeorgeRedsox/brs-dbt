{{
    config(
        cluster_by=['email']
      , materialized= 'view'
    )
}}

SELECT * EXCEPT(row_num)
FROM (
    SELECT  email_address AS email
          , team_nickname
          , wh.first_name
          , wh.last_name
          , wh.country
          , wh.state
          , wh.city
          , wh.zip
          , wh.address
          , wh.phone_number
          , identity_point_id
          , guid
          , interaction_count
          , first_interaction_date
          , last_interaction_date
          , spend AS oneview_spend
          , CAST(commerce_billable_forms_spend AS FLOAT64) AS commerce_spend
          , CAST(shop_spend AS FLOAT64) AS shop_spend
          , stubhub_spend
          , primary_ticketing_spend
          , CAST(mlbtv_spend AS FLOAT64) AS mlbtv_spend
          , okta_id
          , CAST(primary_ticket_account_id AS INT64) AS primary_ticket_account_id
          , ticketing_rfm_score
          , shop_rfm_score
          , commerce_rfm_score
          , total_rfm_score
          , avidity_score
          , avidity_classification
          , mlbtv_ltv
          , tickets_ltv
          , shop_ltv
          , fs_nonrenewal_probability
          , team_id
          , ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY avidity_score DESC) AS row_num

    FROM    {{ source('wheelhouse_red_sox', 'customer_summary_agg') }} wh
)
WHERE row_num = 1
  
