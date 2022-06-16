SELECT  Id AS salesforce_opportunity_product_id
      , Nro_of_Seats__c AS nro_of_seats
      , BEO_Lines_Name__c AS beo_lines_name
      , BEO_Section_Sort_Order__c AS beo_section_sort_order
      , Booked_Quantity__c AS booked_quantity
      , BRS_Import_ID__c AS brs_import_id
      , Cleaned_Description__c AS cleaned_description
      , Is_Corporate_Partnership__c AS is_corporate_partnership
      , CreatedById AS created_by_id
      , ServiceDate AS service_date
      , Description__c AS description__c
      , NULLIF(Event_Inventory__c, 'None')  AS event_inventory
      , NULLIF(Fenway_Event__c, 'None') AS fenway_event
      , Fenway_Event_Date_Name__c AS fenway_event_date_name
      , First_Seat__c AS first_seat
      , IF(LENGTH(IF(Game_Date__c IN ('None', 'nan'), NULL, Game_Date__c)) IN (13, 15), 
            TIMESTAMP_MILLIS(CAST( CAST(IF(Game_Date__c IN ('None', 'nan'), NULL, Game_Date__c) AS NUMERIC) AS INT64) ) , 
            TIMESTAMP(IF(Game_Date__c IN ('None', 'nan'), NULL, Game_Date__c)) ) AS game_date
      , General_Location__c AS general_location
      , Group_Buyer_Type__c AS group_buyer_type
      , Group_Ticket_Location__c AS group_ticket_location
      , Name__c AS name__c
      , LastModifiedById AS last_modified_by_id
      , Last_Seat__c AS last_seat
      , Description AS description
      , ListPrice AS list_price
      , Number_of_Guests__c AS number_of_guests
      , NULLIF(OpportunityId, 'None') AS salesforce_opportunity_id
      , Name AS name
      , Patron__c AS patron
      , Plan__c AS plan
      , Premium_Location__c AS premium_location
      , Premium_Section__c AS premium_section
      , NULLIF(Product2Id, 'None') AS product_id
      , ProductCode AS product_code
      , Product_Display_Name__c AS product_display_name
      , ProVenue_Account_ID__c AS provenue_account_id
      , ProVenue_Order_ID__c AS provenue_order_id
      , Quantity AS quantity
      , Sales_Path__c AS sales_path
      , UnitPrice AS unit_price
      , Season__c AS season
      , Status__c AS status
      , Tax_Amount__c AS tax_amount
      , Tax_Rate__c AS tax_rate
      , Taxable__c AS taxable
      , CAST(TotalPrice AS FLOAT64) AS total_price
      , IF(Disable_New_Overwrite__c = 'True', TRUE, FALSE) AS disable_new_overwrite
      , FSE__c AS fse
      , IF(IsDeleted = 'True', TRUE, FALSE) AS is_deleted
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , TIMESTAMP_SECONDS(SAFE_CAST(imported_at_ts AS INT64)) AS imported_at_ts
      , IF(LENGTH(NULLIF(CreatedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(CreatedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(CreatedDate, 'None'))) AS created_ts
      , attributes_type
      , attributes_url

FROM    {{ source('stage', 'salesforce__brs__opportunity_products') }}

