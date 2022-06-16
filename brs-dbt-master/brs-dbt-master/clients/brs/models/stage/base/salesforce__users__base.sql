SELECT  Id AS salesforce_user_id
      , IF(IsActive = 'True', TRUE, FALSE) AS is_active
      , ContactId AS contact_id 
      , Name AS name
      , CAST(NULLIF(ProVenue_User_ID__c, 'None') AS INT64)  AS provenue_user_id
      , UserRoleId AS user_role_id
      , Title AS title
      , User_ID__c AS user_id
      , Username AS username
      , attributes_type
      , attributes_url
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts

FROM    {{ source('stage', 'salesforce__brs__users') }}
-- WHERE   IsActive = 'True'
