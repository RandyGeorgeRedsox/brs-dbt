SELECT  shipping_Country AS shipping_country
      , shipping_City AS shipping_city
      , shipping_Street AS shipping_street
--       , shipping_First_Name
      , favorite_ballpark_snack AS favorite_ballpark_snack
      , favourite_school_subject AS favorite_school_subject
--       , favourite_player
      , noOfAssignedTickets AS num_assigned_tickets
      , ticketRedemptionCode AS ticket_redemption_code
      , guardian_Phone_Cell AS guardian_phone_cell
      , date_Upgraded AS date_upgraded
      , guardian_Email AS guardian_email
      , guardian_Address_Country AS guardian_address_country
      , guardian_Address_Zipcode AS guardian_address_zipcode
      , activationDate AS activation_date
      , guardian_Address_State AS guardian_address_state
      , guardian_Phone AS guardian_phone
      , guardian_Address_City AS guardian_address_city
      , shipping_State AS shipping_state
      , guardian_Address_Street AS guardian_address_street
--       , shipping_Last_Name
      , guardian_MemberId AS guardian_member_id
      , guardian_Lastname AS guardian_lastname
      , upgraded
      , guardian_Firstname AS guardian_firstname
--       , guardian_Card_Number
      , shipping_Zip_Code AS shipping_zip_code
      , child_Gender AS child_gender
      -- , guardian_Gender
      , email
      , child_PatronID AS child_patron_id
      , child_Barcode AS child_barcode
      , chid_Surname AS child_surname
      , date_Modified AS date_modified
      , membershipType AS membership_type
      , child_CreationDate AS child_creation_date
      , guardian_DOB AS guardian_dob
      , guardian_Title AS guardian_title
--       , guardian_ExternalRefNumber
      , child_DateOfBirth AS child_date_of_birth
      , child_MemberID AS child_member_id
      , child_Card_Number AS child_card_number
--       , child_AccountPaymentState
      , child_FirstName AS child_firstname
--       , recordID

FROM    {{ source('fortress__stage', 'kidnation') }}