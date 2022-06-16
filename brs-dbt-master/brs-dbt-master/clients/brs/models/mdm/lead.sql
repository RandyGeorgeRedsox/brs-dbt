{{
    config(
        cluster_by=['form_id'],
        enabled=false
    )
}}

--MLB Brochure Download
SELECT  form_id
      , email
      , first_name
      , last_name
      , company
      , birthdate
      , mobile_phone
      , phone
      , NULL AS number_of_seats
      , NULL AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , '' AS plans_interested_in
      , '' AS purpose_of_tickets
      , '' AS in_person_appointment_interest
      , NULL AS type_of_event
      , NULL AS time_requested
      , NULL AS date_requested
      , NULL AS description
      , NULL AS group_sales_type
      , NULL AS seat_location
      , NULL AS party_area
      , '' AS accessible_seating_required
      , NULL AS how_did_you_hear_about_us
      , NULL AS preferred_game_date
      , NULL AS group_name
      , NULL AS secondary_group_location
      , NULL AS secondary_party_area
      , NULL AS secondary_group_date

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id = '0001551903676569-12f8e2486200-0001'

UNION ALL

--Fenway Premium Suites Form
SELECT  form_id
      , email
      , first_name
      , last_name
      , company
      , birthdate
      , mobile_phone
      , phone
      , group_size AS number_of_seats
      , SUBSTR(comments, 0, 300) AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , '' AS plans_interested_in
      , '' AS purpose_of_tickets
      , '' AS in_person_appointment_interest
      , NULL AS type_of_event
      , NULL AS time_requested
      , NULL AS date_requested
      , NULL AS description
      , NULL AS group_sales_type
      , NULL AS seat_location
      , NULL AS party_area
      , '' AS accessible_seating_required
      , NULL AS how_did_you_hear_about_us
      , NULL AS preferred_game_date
      , NULL AS group_name
      , NULL AS secondary_group_location
      , NULL AS secondary_party_area
      , NULL AS secondary_group_date

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id='0001541260078637-12823d363fa4-0001'
AND     mobile_phone IS NOT NULL
AND     mobile_phone != 'redsoxalerts'

UNION ALL

--New Season Ticket Inquiries Form
SELECT  form_id
      , email
      , first_name
      , last_name
      , company
      , birthdate
      , mobile_phone
      , phone
      , quantity_of_seats AS number_of_seats
      , SUBSTR(additional_comments, 0, 300) AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , CASE WHEN plans_interested_in = 'full, partial' THEN 'Full Season Ticket Packages; Partial Season Ticket Packages'
             WHEN plans_interested_in = 'partial' THEN 'Partial Season Ticket Packages'
             WHEN plans_interested_in = 'full' THEN 'Full Season Ticket Packages'
             WHEN plans_interested_in = 'premium' THEN 'Premium Season Tickets (Dugout, Dell EMC Club, and State Street Pavilion Club)'
             WHEN plans_interested_in = 'premium, full' THEN 'Premium Season Tickets (Dugout, Dell EMC Club, and State Street Pavilion Club); Full Season Ticket Packages'
             WHEN plans_interested_in = 'premium, partial' THEN 'Premium Season Tickets (Dugout, Dell EMC Club, and State Street Pavilion Club); Partial Season Ticket Packages'
             WHEN plans_interested_in = 'premium, full, partial' THEN 'Premium Season Tickets (Dugout, Dell EMC Club, and State Street Pavilion Club); Full Season Ticket Packages; Partial Season Ticket Packages'
        END AS plans_interested_in
      , purpose_of_tickets
      , in_person_appointment_interest
      , NULL AS type_of_event
      , NULL AS time_requested
      , NULL AS date_requested
      , NULL AS description
      , NULL AS group_sales_type
      , NULL AS seat_location
      , NULL AS party_area
      , '' AS accessible_seating_required
      , NULL AS how_did_you_hear_about_us
      , NULL AS preferred_game_date
      , NULL AS group_name
      , NULL AS secondary_group_location
      , NULL AS secondary_party_area
      , NULL AS secondary_group_date
      

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id = '0001501701662527-aee5cc2ffffccf4-0001' 
AND     mobile_phone IS NOT NULL
AND     mobile_phone != 'redsoxalerts'

UNION ALL

--Fenway Events Inquiries Form
SELECT  form_id
      , email
      , first_name
      , last_name
      , company
      , birthdate
      , mobile_phone
      , phone
      , number_of_guests AS number_of_seats
      , NULL AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , '' AS plans_interested_in
      , '' AS purpose_of_tickets
      , '' AS in_person_appointment_interest
      , type_of_event
      , time_requested
      , date_requested
      , description
      , NULL AS group_sales_type
      , NULL AS seat_location
      , NULL AS party_area
      , '' AS accessible_seating_required
      , NULL AS how_did_you_hear_about_us
      , NULL AS preferred_game_date
      , NULL AS group_name
      , NULL AS secondary_group_location
      , NULL AS secondary_party_area
      , NULL AS secondary_group_date

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id='0001541604669823-e360e933230-0001'
AND     mobile_phone IS NOT NULL
AND     mobile_phone != 'redsoxalerts'

UNION ALL

--Group Ticket Information Form
SELECT  form_id
      , email
      , first_name
      , last_name
      , group_name AS company
      , birthdate
      , mobile_phone
      , phone
      , number_of_guests AS number_of_seats
      , SUBSTR(comments, 0, 300) AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , '' AS plans_interested_in
      , '' AS purpose_of_tickets
      , '' AS in_person_appointment_interest
      , NULL AS type_of_event    
      , NULL AS time_requested
      , NULL AS date_requested
      , NULL AS description
      , group_sales_type
      , seat_location
      , party_area
      , accessible_seating_required
      , how_did_you_hear_about_us
      , preferred_game_date
      , NULL AS group_name
      , NULL AS secondary_group_location
      , NULL AS secondary_party_area
      , NULL AS secondary_group_date

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id='0001541176745222-a665ba05d7a-0001'
AND     mobile_phone IS NOT NULL
AND     mobile_phone != 'redsoxalerts'

UNION ALL
--Group Ticket Renewal Form
SELECT  form_id
      , email
      , first_name
      , last_name
      , company
      , birthdate
      , mobile_phone
      , phone
      , number_of_guests AS number_of_seats
      , SUBSTR(comments, 0, 300) AS additional_comments
      , created_date
      , address
      , lead_source
      , webform_lead_id
      , '' AS plans_interested_in
      , '' AS purpose_of_tickets
      , '' AS in_person_appointment_interest
      , NULL AS type_of_event    
      , NULL AS time_requested
      , NULL AS date_requested
      , NULL AS description
      , group_sales_type
      , seat_location
      , party_area
      , accessible_seating_required
      , how_did_you_hear_about_us
      , preferred_game_date
      , SUBSTR(group_name, 0, 80) AS group_name
      , secondary_group_location
      , secondary_party_area
      , secondary_group_date

FROM    {{ ref('wheelhouse__forms2_detail__latest') }}
WHERE   form_id='0001637596045328-b675121bffffa6eb-0001'
AND     mobile_phone IS NOT NULL
AND     mobile_phone != 'redsoxalerts'
