SELECT  forward_reporting_activity_id
      , forward_event_description
      , sender_patron_id
      , sender_patron_guid
      , recipient_patron_id
      , recipient_patron_guid
      , forward_id
      , ticket_section
      , ticket_row
      , ticket_seat
      , forward_timestamp
      , ticket_id
      , mlb_event_id
      , send_to_email
      , share_link_only
      , sender_patron_okta_id
      , recipient_patron_okta_id
      , batch_date
      , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'ticket_forwarding') }}