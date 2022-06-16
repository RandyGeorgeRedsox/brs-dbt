{{
    config(
        cluster_by=['attending_account_id']
      , materialized= 'table'
    )
}}

WITH forward AS (
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT * 
              , ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY forward_timestamp DESC) AS row_num
        FROM {{ ref('wheelhouse__ticket_forwarding__latest') }}
        WHERE forward_event_description = 'ACCEPTED'
    )
    WHERE row_num = 1
)

, scan AS (
    SELECT * 
    FROM {{ ref('wheelhouse__ticket_scan_tdc__latest') }}
    WHERE scan_result_type_description IN ('VALID_ENTRY', 'VALID_EXIT')
)

SELECT DISTINCT attending_account_id
    --  , attending_email
     , is_attendee
FROM (
    SELECT DISTINCT source_financial_patron_id AS attending_account_id
                -- , financial_patron_email AS attending_email
                , TRUE AS is_attendee
    FROM {{ ref('wheelhouse__ticket_details__latest') }} AS t
        LEFT OUTER JOIN forward AS f
            ON t.source_ticket_id = SAFE_CAST(f.ticket_id AS INT64)
        INNER JOIN scan AS s
            ON t.source_ticket_id = s.ticket_id
    WHERE f.ticket_id IS NULL

    UNION ALL

    SELECT DISTINCT CAST(s.attending_patron_id AS INTEGER) AS attending_account_id
                -- , s.attending_patron_tdc_email AS attending_email
                , TRUE AS is_attendee
    FROM {{ ref('wheelhouse__ticket_details__latest') }} AS t
        INNER JOIN forward AS f
            ON t.source_ticket_id = SAFE_CAST(f.ticket_id AS INT64)
        INNER JOIN scan AS s
            ON t.source_ticket_id = s.ticket_id
)