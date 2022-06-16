SELECT
    Id AS salesforce_fenway_event_id
  , Active__c AS is_active
  , CreatedById AS created_by_id
  , Event_Code__c AS event_code
  -- , DATE(IF(Event_Date__c = 'None', NULL, Event_Date__c)) AS event_date
  , PARSE_DATE('%m/%d/%Y', IF(Event_Date__c = 'None', NULL, Event_Date__c)) AS event_date
  , IF(LENGTH(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c)) IN (13, 15), 
            TIMESTAMP_MILLIS(CAST( CAST(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c) AS NUMERIC) AS INT64) ) , 
            TIMESTAMP(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c)) ) AS event_date_time
  , Event_Description__c AS event_description
  , Event_GUID__c AS event_guid
  , Event_ID__c AS event_id
  , Event_Type__c AS event_type
  , External_Event_ID__c AS external_event_id
  , Name AS event_name
  , Homestand__c AS homestand
  , LastModifiedById AS last_modified_by_id
  , Last_Run_Event_Inventory_Creation__c AS last_run_event_inventory_creation
  , Opponent__c AS opponent
  , OwnerId AS owner_id
  , Price_Scale__c AS price_scale
  , Price_Tier__c AS price_tier
  , Season__c AS season
  , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
  , attributes_type AS attributes_type
  , attributes_url AS attributes_url
  , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
FROM {{ source('stage', 'salesforce__brs__fenway_events') }}
