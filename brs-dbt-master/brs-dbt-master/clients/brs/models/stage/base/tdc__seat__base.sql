select  seat_id as seat_id
--       , master_config_seat_index
      , row_ as seat_row
      , seat_number as seat_number
--       , cell_x
--       , cell_y
--       , master_config_id
--       , seat_layout_id
      , section_id as section_id
--       , created_date
      , last_updated_date as last_updated_ts
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts

    
from    {{ source('wheelhouse_red_sox_ticketing', 'seat') }}