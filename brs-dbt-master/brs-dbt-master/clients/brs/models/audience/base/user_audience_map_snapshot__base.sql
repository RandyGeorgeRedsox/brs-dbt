SELECT  audience_id
      , snapshot_date
      , audience_name
      , salesforce_audience_export_email
      , salesforce_audience_export_campaign_name
      , salesforce_audience_export_campaign_id
    --   , salesforce_audience_export_task_comment
    --   , salesforce_audience_export_task_due_datetime
    --   , salesforce_audience_export_contact_id
    --   , salesforce_audience_export_company
      , salesforce_audience_export_last_name
      , salesforce_audience_export_first_name
    --   , pv_contact_id
    --   , sf_contact_id
      , _composite_key
      , pv_account_id
    --   , pv_account_created_date
    --   , account_email
    --   , first_purchase_date
    --   , last_purchase_date
    --   , average_purchase_frequency
    --   , is_routed_to_salesforce
    --   , current_season_ticket_holder
    --   , lapsed_season_ticket_holder
    --   , current_partial_plan_buyer
    --   , lapsed_partial_plan_buyer
    --   , current_group_buyer
    --   , lapsed_group_buyer
    --   , has_reserved_season_tickets
      , pv_country AS country
    --   , total_lifetime_spend
    --   , current_year_package_tickets_spend
    --   , current_year_group_tickets_spend
    --   , current_year_single_tickets_spend
    --   , last_year_total_tickets_spend
    --   , days_since_last_interaction
    --   , shop_spend
    --   , commerce_spend
    --   , mlbtv_spend
    --   , oneview_spend
    --   , total_tickets_purchased
    --   , tickets_purchased_2016
    --   , games_purchased_2016
    --   , tickets_purchased_2017
    --   , games_purchased_2017
    --   , tickets_purchased_2018
    --   , games_purchased_2018
    --   , tickets_purchased_2019
    --   , games_purchased_2019
    --   , tickets_purchased_2020
    --   , games_purchased_2020
    --   , is_new_season_ticket_inquiries_lead
    --   , is_group_tickets_lead
    --   , is_fenway_premium_suites_lead
    --   , is_mlb_brochure_download_lead
    --   , is_designated_likely_broker
    --   , is_designated_sponsor
    --   , is_designated_mlb
    --   , is_designated_trade
    --   , is_designated_internal
      , first_name
      , last_name
    --   , city
    --   , state
      , zip AS postal_code
      , email
    --   , ticketing_rfm_score
    --   , shop_rfm_score
    --   , commerce_rfm_score
    --   , total_rfm_score
    --   , avidity_score
    --   , avidity_classification
    --   , company
      , is_control
      , user_id

FROM    {{ source('audience', 'user_audience_map_snapshot') }}
