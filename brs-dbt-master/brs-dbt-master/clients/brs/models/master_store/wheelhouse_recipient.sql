SELECT f.recipient_patron_id
     , f.recipient_patron_guid
     , f.send_to_email AS email
     , f.recipient_patron_okta_id
     , DATE(f.forward_timestamp) AS ticket_receive_date
     , f.ticket_id
     , f.mlb_event_id
     , IFNULL(g.game_description, CONCAT(g.away_team, ' VS ', g.home_team)) AS recipient_event
     , g.home_team
     , g.away_team
     , DATE(g.game_time) AS recipient_event_date
FROM (
SELECT * 
    , ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY forward_timestamp DESC) AS row_num
FROM {{ ref('wheelhouse__ticket_forwarding__latest') }}
WHERE forward_event_description = 'ACCEPTED'
) AS f
  LEFT OUTER JOIN {{ ref('wheelhouse__baseball_game_master__base') }} AS g 
      ON f.mlb_event_id = g.game_pk 
WHERE row_num = 1