SELECT  email_addr AS email
    --   , age
    --   , gender
      , income
    --   , marital_status
      , family_with_kids
      , kids_in_household
    --   , race_ethnicity
      , last_acxiom_update_date
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'acxiom_demographics') }}