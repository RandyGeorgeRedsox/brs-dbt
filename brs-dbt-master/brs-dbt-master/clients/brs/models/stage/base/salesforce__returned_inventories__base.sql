SELECT  Id AS salesforce_returned_inventories_id
      , CreatedById AS created_by_id
      , Event_Inventory__c AS event_inventory
      , LastModifiedById AS last_modified_by_id
      , Opportunity__c AS salesforce_opportunity_id
      , Name AS name
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , attributes_type
      , attributes_url
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
FROM    {{ source('stage', 'salesforce__brs__returned_inventories') }}
