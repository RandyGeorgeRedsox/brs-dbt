SELECT  NULLIF(Account_Name__c, 'None') AS account_name
      , IF(Active__c = 'True', TRUE, FALSE) AS is_active
      , Availability__c AS availability
      , Id AS salesforce_event_inventory_id
      , CreatedById AS created_by_id
      , NULLIF(Customer__c, 'None') AS customer
      , Day_of_Week__c AS day_of_week
      , NULLIF(Event_Date__c, 'None') AS event_date
      , DATETIME(
        CASE WHEN 
          LENGTH(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c)) = 13 
        THEN  
          TIMESTAMP_MILLIS(CAST(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c) AS INT64)) 
        WHEN 
          LENGTH(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c)) = 15
        THEN 
          TIMESTAMP_MILLIS(CAST(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, SUBSTR(Event_Date_Time__c, 0, 13)) AS INT64))
        ELSE
          TIMESTAMP(IF(Event_Date_Time__c IN ('None', 'nan'), NULL, Event_Date_Time__c))
        END) AS event_date_time
      , NULLIF(Fenway_Events__c, 'None') AS fenway_events
      , NULLIF(Homestand__c, 'None') AS homestand
      , NULLIF(Hospitality_Location__c, 'None') AS hospitality_location
      , Name AS name
      , LastModifiedById AS last_modified_by_id
      , NULLIF(Mailing_Account__c, 'None')  AS mailing_account
      , NULLIF(Mailing_City__c, 'None')  AS mailing_city
      , NULLIF(Mailing_Contact__c, 'None')  AS mailing_contact
      , NULLIF(Mailing_Country__c, 'None')  AS mailing_country
      , NULLIF(Mailing_State__c, 'None')  AS mailing_state
      , NULLIF(Mailing_Street__c, 'None')  AS mailing_street
      , NULLIF(Mailing_Zip__c, 'None')  AS mailing_zip
      , Max_Number_Seated__c AS max_number_seated
      , Meeting_Location__c AS meeting_location
      , Name__c AS name__c
      , Max_Number_of_Guests__c AS max_number_of_guests
      , NULLIF(Opponent__c, 'None')  AS opponent
      , NULLIF(Opportunity__c, 'None') AS opportunity
      , NULLIF(Package__c, 'None')  AS package
      , NULLIF(Patron__c, 'None')  AS patron
      , NULLIF(Premium_Location__c, 'None')  AS premium
      , NULLIF(Price_Scale__c, 'None')  AS price_scale
      , Product_Type__c AS product_type
      , Product_Model__c AS product_model
      , NULLIF(Tickets__c, 'None')  AS tickets
      , NULLIF(Total_Cost__c, 'nan')  AS total_cost
      , NULLIF(Tickets_Mailed_On__c, 'None')  AS ticket_mailed_on
      , IF(IsDeleted = 'True', TRUE, FALSE) AS is_deleted
      , Product__c AS product_id 
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , attributes_type AS attributes_type
      , attributes_url AS attributes_url
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts

FROM    {{ source('stage', 'salesforce__brs__event_inventories') }}
