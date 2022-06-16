SELECT  NULLIF(Id, 'None') AS salesforce_member_id
      , Fortress_Member_ID__c AS fortress_member_id
      , Membership_Type__c AS membership_type
      , First_Name__c AS first_name
      , Last_Name__c AS last_name
      , attributes_type
      , attributes_url
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts

FROM    {{ source('stage', 'salesforce__brs__members') }}
