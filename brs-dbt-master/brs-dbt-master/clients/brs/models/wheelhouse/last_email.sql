{{
    config(
        cluster_by=['email']
      , materialized= 'table'
    )
}}

WITH last_email_campaign AS (
SELECT * EXCEPT (row_num)
FROM (
SELECT  email
      , campaign_name AS last_email_campaign_interacted_with
      , ROW_NUMBER() OVER(PARTITION BY email ORDER BY event_time DESC) AS row_num
FROM    {{ ref('wheelhouse__zetahub_events__latest') }}
WHERE event_type IN ('campaign_clicked', 'campaign_opened', 'campaign_bounced')
)
WHERE row_num = 1
)

, last_email_openned AS (
SELECT * EXCEPT (row_num)
FROM (
SELECT  email
      , tag_campaign AS last_email_opened
      , ROW_NUMBER() OVER(PARTITION BY email ORDER BY event_time DESC) AS row_num
FROM    {{ ref('wheelhouse__zetahub_events__latest') }}
WHERE event_type = 'campaign_opened'
)
WHERE row_num = 1
)


SELECT  DISTINCT le.email
      , last_email_opened
      , last_email_campaign_interacted_with
FROM last_email_openned AS le
     FULL OUTER JOIN last_email_campaign AS l
         ON le.email = l.email
