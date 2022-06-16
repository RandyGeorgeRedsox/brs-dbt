SELECT  event_id
      , source_event_id
      , home_team
      , away_team
      , event_date
      , event_desc
      , event_type_desc
      , mlb_game_pk
      , ignore_flag
      , dw_modified_date
      , event_code
      , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'ticket_details_events_tdc') }} 