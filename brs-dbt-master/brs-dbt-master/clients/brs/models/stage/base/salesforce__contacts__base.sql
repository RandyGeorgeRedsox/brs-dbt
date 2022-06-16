SELECT  Id AS contact_id
      , NULLIF(Email, 'None') AS email
      , AccountId AS account_id
      , Title AS title
      , Name AS contact_name
      , Phone AS phone
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
      , SAFE_CAST(NULLIF(ProVenue_Contact_ID__c, 'None') AS INT64) AS provenue_contact_id
      , IF(Flywheel_ID__c = 'None', NULL, Flywheel_ID__c) AS flywheel_id
      , Import__c AS import
    --   , source_file
    --   , attributes_type
    --   , attributes_url

FROM    {{ source('stage', 'salesforce__brs__contacts') }}
