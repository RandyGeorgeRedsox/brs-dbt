SELECT 
        -- email_id
      DISTINCT email_address
    --   , firstname
    --   , lastname
    --   , dob
    --   , address_1
    --   , address_2
    --   , city
    --   , state
    --   , postal_code
    --   , country
    --   , phone1
    --   , phone2
    --   , phone3
      , source
    --   , shop_buyer
    --   , mlbam_identity_point_guid
    --   , mlbtv_buyer
    --   , okta_id
      , IF(active = 'Y', TRUE, FALSE) AS is_active
    --   , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'registered_user_full') }}