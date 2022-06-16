SELECT  
        'Contact' AS object_type,
      , 'Id' AS id_field
      , _composite_key AS Flywheel_ID__c
      , salesforce.contact_id AS Id
      , provenue.first_name	FirstName
      , provenue.last_name AS LastName
      , provenue.birthdate AS Birthday__c
      , salesforce.email AS Email
      , salesforce.contact_id AS Id
      , salesforce.contact_name AS Name
      , salesforce.phone AS Phone
      , provenue.account_id AS ProVenue_Account_ID__c
      , provenue.contact_id AS ProVenue_Contact_ID__c
      , provenue.address.city AS PV_City__c
      , provenue.address.country AS PV_Country__c
      , provenue.email AS PV_Email__c
      , provenue.phone.phone_number AS PV_Phone_Number__c
      , provenue.address.postal_code AS PV_Postal_Code__c
      , provenue.address.state AS PV_State__c
      , provenue.address.street_address_line_1 AS PV_Street_Address__c
      , provenue.address.street_address_line_2 AS PV_Street_Address_Line_2__c
      , salesforce.title AS Title

FROM    {{ ref('contact') }} as c
          , UNNEST(c.pv_email) as e
          , UNNEST(c.pv_phone) as p
          , UNNEST(c.pv_address) as ad

        LEFT OUTER JOIN {{ ref('account') }} AS a
            ON c.provenue.account_id = a.provenue.account_id

WHERE   c.provenue.is_primary
        AND a.provenue.is_active
