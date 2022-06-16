SELECT  'Account' AS object_type
      , 'Id' AS id_field
      , salesforce.account_id AS Id
      , _composite_key AS Flywheel_ID__c
      , account_owner AS OwnerID
      , account_source AS AccountSource
      , owner_id AS OwnerID
      , provenue.account_id AS ProVenue_Account_ID__c
      , provenue.account_type_code AS ProVenue_Account_Type__c
      , provenue.group_spend AS Total_Group_Amount__c
      , provenue.last_year_spend AS Last_Year_Total_Amount__c
      , provenue.package_spend AS Total_Package_Amount__c
      , provenue.primary_contact.address.city AS PV_City__c
      , provenue.primary_contact.address.country AS PV_Country__c
      , provenue.primary_contact.address.postal_code AS PV_Postal_Code__c
      , provenue.primary_contact.address.state AS PV_State__c
      , provenue.primary_contact.address.street_address_line_1 AS PV_Street_Address__c
      , provenue.primary_contact.address.street_address_line_2 AS PV_Street_Address_Line_2__c
      , provenue.primary_contact.contact_id AS PV_Primary_Contact_ID__c
      , provenue.primary_contact.email AS PV_Email__c
      , provenue.primary_contact.phone AS PV_Phone_Number__c
      , provenue.single_spend AS Total_Single_Amount__c
      , provenue.total_spend AS Lifetime_Total_Amount__c
      , salesforce.account_name AS `Name`
      , salesforce.datacom_key AS Jigsaw
      , salesforce.description AS `Description`
      , salesforce.industry AS Industry
      , salesforce.number_of_employees AS NumberOfEmployees
      , salesforce.parent_account_id AS ParentId
      , salesforce.phone AS Phone
      , salesforce.sic_description AS SicDesc
      , salesforce.website AS Website
      
      , COALESCE(salesforce.created_by_id, '0052h000000jxnOAAQ') AS CreatedById  -- sandbox
      -- , COALESCE(salesforce.created_by_id, '0054P00000CLocBQAT') AS CreatedById  -- prod
      , '0052h000000jxnOAAQ' AS LastModifiedById  -- sandbox
      -- , '0054P00000CLocBQAT' AS LastModifiedById  -- prod

FROM    {{ ref('account') }}
WHERE   provenue.is_active = 1
