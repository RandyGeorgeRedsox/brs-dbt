select  section_id as section_id
--       , created_by_operator_id
--       , created_date
--       , last_updated_by_operator_id
      , last_updated_date as last_updated_ts
      , section_code as section_code
      , description as description 
--       , display_order
--       , active
      , public_description as public_description
--       , neighborhood_id
--       , report_section_group_id
--       , venue_id
--       , public_synopsis
--       , short_public_description
--       , public_section_code
            -- , timestamp_seconds(etl_batch_id) as etl_batch_ts
 

from    {{ source('wheelhouse_red_sox_ticketing', 'section') }}