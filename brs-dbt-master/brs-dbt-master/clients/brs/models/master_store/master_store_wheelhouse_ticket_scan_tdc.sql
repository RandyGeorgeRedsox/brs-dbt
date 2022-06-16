{{
    config(
        materialized="table"
    )
}}
SELECT  s.*
      , b.away_team  
FROM {{ ref('wheelhouse__ticket_scan_tdc__latest')}} AS s
LEFT OUTER JOIN {{ ref('wheelhouse__baseball_game_master__latest') }} AS b
    ON s.game_pk = b.game_pk