SELECT  Flywheel_ID__c AS flywheel_id
      , Member__c AS salesforce_member_id
      , Membership_Type__c AS membership_type
      , Age_Range__c AS age_range
      , Handedness__c AS handedness
      , Rookie_Delivery_Type__c AS rookie_delivery_type
      , Active__c AS is_active
      , Season__c AS season
      , Product2Id AS product_id
      , attributes_type
      , attributes_url
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts

FROM    {{ source('stage', 'salesforce__brs__assets') }}
