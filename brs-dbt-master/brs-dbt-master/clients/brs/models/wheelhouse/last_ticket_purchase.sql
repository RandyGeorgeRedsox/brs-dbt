WITH last_purchase AS (
SELECT  *
      , ROW_NUMBER() OVER (PARTITION BY email ORDER BY last_purchase_date DESC) AS row_num
FROM (
    SELECT  buyer_email AS email
          , 'secondary' AS last_ticket_purchase_platform
          , away_team
          , DATE(confirmation_date) AS last_purchase_date
    FROM    {{ ref('wheelhouse__stubhub_details__latest') }}
    WHERE buyer_email IS NOT NULL 
    AND sale_amount !=0 
    UNION ALL

    SELECT  t.financial_patron_email AS email
          , 'primary' AS last_ticket_purchase_platform
          , e.away_team
          , DATE(order_date) AS last_purchase_date
    FROM    {{ ref('wheelhouse__ticket_details__latest') }} AS t
           LEFT OUTER JOIN {{ ref('wheelhouse__ticket_details_events_tdc__latest') }} AS e 
             ON t.event_id = e.event_id
    WHERE t.financial_patron_email IS NOT NULL
)
)
, next_game AS (
    SELECT DISTINCT away_team
         , event_id
         , UPPER(REPLACE(event_desc, ' at Red Sox', '')) AS opponent_team
         , event_date
    FROM {{ ref('wheelhouse__ticket_details_events_tdc__latest') }}
    WHERE event_date > CURRENT_DATE() 
    ORDER BY event_date 
    LIMIT 1
)
, next_week AS (
SELECT DISTINCT away_team
      , event_id
      , UPPER(REPLACE(event_desc, ' at Red Sox', '')) AS opponent_team
      , event_date
FROM {{ ref('wheelhouse__ticket_details_events_tdc__latest') }}
WHERE event_date BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK) AND LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK)
)

, next_scheduled_game AS (
SELECT  DISTINCT financial_patron_email AS email
      , IF(event_id = (SELECT event_id FROM next_game), TRUE, FALSE) AS ticket_purchase_to_next_scheduled_game
FROM {{ ref('wheelhouse__ticket_details__latest') }}  t
WHERE t.sale_amount != 0 
AND financial_patron_email IS NOT NULL
UNION ALL
SELECT  DISTINCT buyer_email AS email
      , IF(event_date = (SELECT event_date FROM next_game), TRUE, FALSE) AS ticket_purchase_to_next_scheduled_game
FROM {{ ref('wheelhouse__stubhub_details__latest') }}
WHERE buyer_email IS NOT NULL
)

, tickets_for_next_week AS (
SELECT  DISTINCT financial_patron_email AS email
      , IF(event_id IN (SELECT event_id FROM next_week), TRUE, FALSE) AS have_purchased_tickets_for_next_week
FROM {{ ref('wheelhouse__ticket_details__latest') }}  t
WHERE t.sale_amount != 0 
AND financial_patron_email IS NOT NULL
UNION ALL
SELECT  DISTINCT buyer_email AS email
      , IF(event_date IN (SELECT event_date FROM next_week), TRUE, FALSE) AS have_purchased_tickets_for_next_week
FROM {{ ref('wheelhouse__stubhub_details__latest') }}
WHERE buyer_email IS NOT NULL
)

, cte_a AS (
SELECT * 
FROM (
SELECT  *
      , ROW_NUMBER() OVER (PARTITION BY email ORDER BY ticket_purchase_to_next_scheduled_game DESC) AS row_num_1
FROM next_scheduled_game
)
WHERE row_num_1 = 1
)

, cte_b AS (
SELECT * 
FROM (
SELECT  *
      , ROW_NUMBER() OVER (PARTITION BY email ORDER BY have_purchased_tickets_for_next_week DESC) AS row_num_2
FROM tickets_for_next_week
)
WHERE row_num_2 = 1
)

SELECT  l.email
      , l.last_ticket_purchase_platform
      , l.away_team AS last_ticket_opponent_purchased
      , l.last_purchase_date
      , IF(l.away_team = (SELECT away_team FROM next_game) OR l.away_team = (SELECT opponent_team FROM next_game), TRUE, FALSE) AS last_ticket_opponent_is_next_scheduled_opponent
      , ticket_purchase_to_next_scheduled_game
      , have_purchased_tickets_for_next_week
FROM last_purchase l
     LEFT OUTER JOIN cte_a
        USING (email)
     LEFT OUTER JOIN cte_b
        USING (email)
WHERE row_num = 1
