SELECT  Section AS section
      , Row AS row
      , Seat AS seat
      , CenterX AS centerx
      , CenterY AS centery

FROM    {{ source('Fenway_Section_Map', 'Seat_Map') }}