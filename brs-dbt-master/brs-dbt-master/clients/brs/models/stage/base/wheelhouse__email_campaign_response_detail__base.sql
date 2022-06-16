SELECT  email_campaign_id
      , email_mailing_id
      , email_cell_id
      , email_list_id
      , email_id
      , email_addr
      , event_date
      , event_type
      , click_url_name
    --   , device_name
    --   , device_category
      , batch_date
      , CONCAT(email_mailing_id, email_id, event_date, IFNULL(click_url_name, '')) AS primary_key
    --   , team_nickname
    --   , team_id
    --   , url_id
FROM    {{ source('wheelhouse_red_sox', 'email_campaign_response_detail') }}
