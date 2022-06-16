
select
    Id AS salesforce_products_id
  , IsActive AS is_active
  , BEO_Section__c AS beo_section
  , Calculate_Price_for_Rain_Out__c AS calculated_price_for_rain_out
  , CreatedById AS created_by_id
  , Display_Name__c AS display_name
  , DisplayUrl AS display_url
  , Do_Not_Display_on_Conga_Templates__c AS do_not_display_on_conga_templates
  , Do_Not_Print_On_BEO__c AS do_not_print_on_beo
  , Do_Not_Print_On_Check__c AS do_not_print_on_check
  , ExternalDataSourceId AS external_data_source_id
  , ExternalId AS external_id
  , FE_Sort_Order__c AS fe_sort_order
  , Flywheel_ID__c AS flywheel_id
  , FSE__c AS fse
  , Guest_Standing_Capacity__c AS guest_standing_capacity
  , Hide_Price_On_BEO__c AS hide_price_on_beo
  , Hide_Quantity_On_BEO__c AS hide_quantity_on_beo
  , Homestand_Display_Name__c AS homestand_display_name
  , Homestand_Sort_Order__c AS homestand_sort_order
  , Hospitality_Location__c AS hospitality_location
  , Is_Additional_Ticket__c AS is_additional_ticket
  , Item_Category__c AS item_category
  , Item_Type__c AS item_type
  , LastModifiedById AS last_modified_by_id
  , Last_Run_Event_Inventory_Creation__c AS last_run_event_inventory_creation
  , Master_Source__c AS master_source
  , Max_Number_of_Guests__c AS max_number_of_guests
  , Max_Number_Seated__c AS max_number_seated
  , Meeting_Location__c AS meeting_location
  , Plan__c AS plan 
  , Premium_Location__c AS premium_location
  , Price_Scale__c AS price_scale
  , Price_Tier__c AS price_tier
  , ProductCode AS product_code
  , Description AS product_description  
  , Family AS family
  , Product_GUID__c AS product_guid
  , Product_ID__c AS product_id
  , Product_Model__c AS product_model
  , Name AS product_name
  , Product_Type__c AS product_type
  , Property__c AS property
  , Revenue_Classification__c AS revenue_classification
  , Rich_Description__c AS rich_description
  , Season__c AS season
  , Show_Price__c AS show_price
  , Show_Quantity__c AS show_quantity
  , Taxable__c AS taxable
  , Ticket_Status__c AS ticket_status
  , Unit_Price__c AS unit_price
  , Units__c AS units
  , Sub_Location__c AS sub_location
  , TIMESTAMP_MILLIS(CAST(LastModifiedDate AS INT64)) AS last_modified_ts
  , attributes_type AS attributes_type
  , attributes_url AS attributes_url
  , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
from {{ source('stage', 'salesforce__brs__products') }}

