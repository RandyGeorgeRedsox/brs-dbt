{{
    config(
        cluster_by=['form_id'],
        materialized='view'
    )
}}

SELECT  form_id
      , campaign_name
    --   , form_title
    --   , form_page_id
    --   , form_page_title
      , form_type
    --   , locale
    --   , form_time_zone
      , status
    --   , tags
    --   , form_live_date
    --   , form_deactivate_date
    --   , created_date
    --   , form_id_version
    --   , updated_date
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'forms2_mapping') }} wh