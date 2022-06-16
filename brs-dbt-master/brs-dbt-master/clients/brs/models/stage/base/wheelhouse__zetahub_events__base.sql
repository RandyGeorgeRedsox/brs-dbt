SELECT  event_id
      -- , campaign_id
      , event_type
      , event_time
      -- , partner_id
      -- , query_id
      , campaign_name
      -- , subject_line
      -- , channel
      , email
      -- , device_type
      -- , campaign_type
      -- , message_time
      -- , message_date
      -- , tag_property
      -- , tag_business_unit
      , tag_campaign
      -- , tag_club
      -- , tag_campaign_type
      -- , additional_tags
      -- , is_hard_bounce
      -- , is_global_optout
      -- , is_unsubscribe
      -- , is_control_group
      -- , os_name
      -- , os_version
      -- , clicked_url
      -- , reporting_name
      -- , batch_date
      -- , team_id
      -- , team_nickname
      -- , message_uid

FROM    {{ source('wheelhouse_red_sox', 'zetahub_events')}}
WHERE email IS NOT NULL
-- AND event_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 YEAR)
