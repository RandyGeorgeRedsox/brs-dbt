SELECT  NULLIF(Id, 'None') AS account_id
      , NULLIF(Name, 'None') AS account_name
      , NULLIF(Type, 'None') AS type
      , NULLIF(Phone, 'None') AS phone
      , NULLIF(SicDesc, 'None') AS sic_description
      , NULLIF(OwnerId, 'None') AS owner_id
      , NULLIF(AccountSource, 'None') AS account_source
      , NULLIF(CreatedById, 'None') AS created_by_id
      , NULLIF(Jigsaw, 'None') AS datacom_key
      , NULLIF(Description, 'None') AS `description`
      , NULLIF(NULLIF(NumberOfEmployees, 'None'), 'nan') AS number_of_employees
      , NULLIF(Industry, 'None') AS industry
      , NULLIF(LastModifiedById, 'None') AS last_modified_by_id
      , NULLIF(ParentId, 'None') AS parent_account_id
      , NULLIF(Website, 'None') AS website
      , SAFE_CAST(NULLIF(ProVenue_Account_ID__c, 'None') AS INT64) AS provenue_account_id
      , NULLIF(attributes_type, 'None') AS attributes_type 
      , NULLIF(attributes_url, 'None') AS attributes_url
      , IF(LENGTH(NULLIF(LastModifiedDate, 'None')) = 13, TIMESTAMP_MILLIS(CAST(NULLIF(LastModifiedDate, 'None') AS INT64)) , TIMESTAMP(NULLIF(LastModifiedDate, 'None'))) AS last_modified_ts
      , TIMESTAMP_SECONDS(CAST(imported_at_ts AS INT64)) AS imported_at_ts
      , CAST(IsDeleted AS BOOL) AS is_deleted
      , NULLIF(PV_Email__c, 'None') AS pv_email
      , NULLIF(BillingStreet, 'None') AS billing_street
      , NULLIF(BillingCity, 'None') AS billing_city
      , NULLIF(BillingState, 'None') AS billing_state
      , NULLIF(BillingPostalCode, 'None') AS billing_postal_code
      , NULLIF(BillingCountry, 'None') AS billing_country
      , NULLIF(ShippingStreet, 'None') AS shipping_street
      , NULLIF(ShippingCity, 'None') AS shipping_city
      , NULLIF(ShippingState, 'None') AS shipping_state
      , NULLIF(ShippingPostalCode, 'None') AS shipping_postal_code
      , NULLIF(ShippingCountry, 'None') AS shipping_country
      -- , source_file


FROM    {{ source('stage', 'salesforce__brs__accounts') }}
WHERE   attributes_type = 'Account'

