{{
    config(
        cluster_by=['ticket_id'],
        materialized= 'table'
    )
}}
SELECT  DISTINCT ticket_id
      , IF(source_ticket_id IN (SELECT DISTINCT ticket_id FROM {{ ref('wheelhouse__ticket_scan_tdc__latest') }}), FALSE, TRUE) is_unused_ticket
FROM {{ ref('wheelhouse__ticket_details__latest') }}
