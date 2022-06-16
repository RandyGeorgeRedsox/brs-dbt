{{
    config(
        cluster_by=['email_address']
      , materialized= 'view'
    )
}}

SELECT  data_source
      , team_nickname
      , interaction_date
      , email_address AS email
    --   , first_name
    --   , last_name
    --   , country
    --   , state
    --   , city
    --   , zip
    --   , address
    --   , phone_number
    --   , identity_point_id
    --   , GUID
    --   , spend
    --   , commerce_billable_forms_spend
    --   , shop_spend
    --   , stubhub_spend
    --   , primary_ticketing_spend
    --   , mlbtv_spend
    --   , team_id

FROM    {{ source('wheelhouse_red_sox', 'all_interactions_vw') }}