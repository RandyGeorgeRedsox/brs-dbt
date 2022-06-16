{{
    config(
        materialized="view"
      , cluster_by=['seat_id']
    )
}}

SELECT  s.seat_id
      , sec.section_id
      , s.seat_number
      , IF(SUBSTR(s.seat_row, 1, 1) = '0', SUBSTR(s.seat_row, 2, 1), s.seat_row) AS seat_row
      , sec.section_code AS seat_section_code
      , sm.centerx AS center_x
      , sm.centery AS center_y
FROM    {{ ref('tdc__seat__base') }} AS s
        LEFT OUTER JOIN {{ ref('tdc__section__base') }} AS sec
            ON s.section_id = sec.section_id
        LEFT OUTER JOIN {{ ref('tdc__seat_map__base') }} AS sm
            ON sec.section_code = sm.section
            AND s.seat_number  = sm.seat 
            AND IF(SUBSTR(s.seat_row, 1, 1) = '0', SUBSTR(s.seat_row, 2, 1), s.seat_row) = sm.row 