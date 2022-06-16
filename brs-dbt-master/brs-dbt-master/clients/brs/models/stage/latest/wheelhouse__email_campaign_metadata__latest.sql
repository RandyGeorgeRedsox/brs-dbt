SELECT  email_cell_id
      , email_list_id
      , email_campaign_name
      , email_mailing_name
    --   , udf2_business_unit
    --   , udf3_campaign
    --   , udf4_mlb_club
    --   , udf5_campaign_type
    --   , email_cell_drop_date
      , email_cell_name
      , cell_subject_line
      , email_list_name
    --   , udf3_team_code
    --   , partner_id
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'email_campaign_metadata') }} 
