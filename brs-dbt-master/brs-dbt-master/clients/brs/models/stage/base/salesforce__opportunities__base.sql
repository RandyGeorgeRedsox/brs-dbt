SELECT  Id AS salesforce_opportunity_id
      , Name AS opportunity_name
      , NULLIF(AccountId, 'None') AS salesforce_account_id
      , NULLIF(CampaignId, 'None') AS campaign_id
      , Description AS description
      , Flywheel_ID__c AS flywheel_id
      , NULLIF(PV_Account_ID__c, 'None') AS provenue_account_id
      , Primary_Opportunity_Email__c AS primary_opportunity_email
      , LeadSource AS lead_source
      , Season__c AS season
      , RecordTypeId AS opportunity_record_type
      , IF(Amount IN ('nan', 'None'), 0, CAST(Amount AS FLOAT64)) AS amount
      , Stage__c AS status
      , StageName AS stage_name
      -- , IF(Stage__c = 'Closed Won', 1, 0) AS stage_flag
      , CloseDate AS close_date
      , OwnerId AS owner_id
      , Owner_Name__c AS owner_name
      , Last_Activity_Date__c AS last_activity_date
      , Preferred_Contact_Method__c AS preferred_contact_method
      , Primary_Contact__c AS primary_contact
      , Top_Contact__c AS top_contact
      , Contact_s_Name__c AS contact_name
      , From_Contact__c AS from_contact
      , Point_of_Contact_Confirmed__c AS point_of_contact_confirmed
      , Payment_Method__c AS payment_method
      , Payment_Plan__c AS payment_option
      , attributes_type 
      , attributes_url
      , IsDeleted AS is_deleted
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
      , IF(LENGTH(NULLIF(CreatedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(CreatedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(CreatedDate, 'None'))) AS created_ts

FROM    {{ source('stage', 'salesforce__brs__opportunities') }}
