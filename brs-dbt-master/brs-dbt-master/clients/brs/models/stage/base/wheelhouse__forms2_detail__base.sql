{{
    config(
        cluster_by=['form_id']
    )
}}

SELECT  a.form_id
      , TRIM(LOWER(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_email'))) AS email
      , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_forename') AS first_name
      , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_surname') AS last_name
      , CASE WHEN a.form_id = '0001541604669823-e360e933230-0001' 
             THEN COALESCE(
            JSON_EXTRACT_SCALAR(form_keywords, '$.organization')
          , CONCAT(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_forename'), ' ', JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_surname'))
        ) 
             WHEN a.form_id = '0001551903676569-12f8e2486200-0001'
             THEN COALESCE(
            JSON_EXTRACT_SCALAR(form_keywords, "$['Group/Company Name']")
          , CONCAT(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_forename'), ' ', JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_surname'))
        ) 
             WHEN a.form_id = '0001637596045328-b675121bffffa6eb-0001'
             THEN COALESCE(
            JSON_EXTRACT_SCALAR(form_keywords, '$.Group Name')
          , CONCAT(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_forename'), ' ', JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_surname'))
        ) 
        ELSE COALESCE(
            NULLIF(COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.Company Name'), JSON_EXTRACT_SCALAR(form_keywords, '$.company-name')), '')
          , CONCAT(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_forename'), ' ', JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_surname'))
        ) END AS company
      , PARSE_DATE('%m/%d/%Y', JSON_EXTRACT_SCALAR(form_keywords, '$.first_person_dob')) AS birthdate
      , JSON_EXTRACT_SCALAR(form_keywords, '$.phone_number_mobile') AS mobile_phone
      , COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.phone'),JSON_EXTRACT_SCALAR(form_keywords, '$.phone_number_mobile')) AS phone
      , created_date
      , JSON_EXTRACT_SCALAR(form_keywords, '$.quantity-of-seats') AS quantity_of_seats -- AS number_of_seats for new_season_ticket_inquiries_form
      , JSON_EXTRACT_SCALAR(form_keywords, '$.Group Size') AS group_size -- AS number_of_seats for fenway_premium_suite_form
      , CASE WHEN a.form_id = '0001541260078637-12823d363fa4-0001'
             THEN JSON_EXTRACT_SCALAR(form_keywords, '$.Comments') 
             WHEN a.form_id = '0001541176745222-a665ba05d7a-0001' OR a.form_id = '0001637596045328-b675121bffffa6eb-0001'
             THEN JSON_EXTRACT_SCALAR(form_keywords, '$.comments') END AS comments -- additional comments for renewal
      , JSON_EXTRACT_SCALAR(form_keywords, '$.additional-comments') AS additional_comments
      , JSON_EXTRACT_SCALAR(form_keywords, '$.what-plans-are-you-interested-in') AS plans_interested_in
      , JSON_EXTRACT_SCALAR(form_keywords, '$.tickets-purpose') AS purpose_of_tickets
      , COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.in-person-appointment'),
                JSON_EXTRACT_SCALAR(form_keywords, '$.virtual-appointment')) AS in_person_appointment_interest
      , STRUCT(
            CONCAT(IFNULL(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_line1'), ''), ' ', IFNULL(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_line2'), '')) AS street
          , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_city') AS city
          , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_state') AS state
          , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_postal_code') AS postal_code
          , JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_address_country') AS country
        ) AS address
      , JSON_EXTRACT_SCALAR(form_keywords, '$.type of event') AS type_of_event
      , CASE WHEN a.form_id = '0001541176745222-a665ba05d7a-0001' OR a.form_id = '0001637596045328-b675121bffffa6eb-0001'
             THEN JSON_EXTRACT_SCALAR(form_keywords, '$.number-of-attendees')
             ELSE JSON_EXTRACT_SCALAR(form_keywords, '$.number of guests') END AS number_of_guests -- number of seats for renewal form
      , JSON_EXTRACT_SCALAR(form_keywords, '$.time requested') AS time_requested
      , JSON_EXTRACT_SCALAR(form_keywords, '$.date requested') AS date_requested
      , JSON_EXTRACT_SCALAR(form_keywords, '$.description') AS description
      , JSON_EXTRACT_SCALAR(form_keywords, '$.inquiry-type') AS group_sales_type

      , CASE WHEN a.form_id = '0001541176745222-a665ba05d7a-0001'
        THEN JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only seat-location') 
        ELSE JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only primary primary-group-location') END AS seat_location
      , JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only primary secondary-group-location') AS secondary_group_location
      , CASE WHEN a.form_id='0001541176745222-a665ba05d7a-0001'
        THEN JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area Party Area') 
        ELSE JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area primary primary-hospitality-area') END AS party_area
      , JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area primary secondary-hospitality-area') AS secondary_party_area
      , JSON_EXTRACT_SCALAR(form_keywords, '$.accessible-seating') AS accessible_seating_required
      , JSON_EXTRACT_SCALAR(form_keywords, '$.Group Name') AS group_name
      , JSON_EXTRACT_SCALAR(form_keywords, '$.How did you hear about Red Sox Group Tickets') AS how_did_you_hear_about_us
      , CASE WHEN a.form_id = '0001637596045328-b675121bffffa6eb-0001'
        THEN COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area primary primary-hospitality-date'), JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only primary primary-group-date'))
        ELSE COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area game-date-hospitality'), 
          JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only game-date-group')) END AS preferred_game_date
      , COALESCE(JSON_EXTRACT_SCALAR(form_keywords, '$.Group Hospitality Area primary secondary-hospitality-date'), 
          JSON_EXTRACT_SCALAR(form_keywords, '$.Group Tickets Only primary secondary-group-date')) AS secondary_group_date
      , b.lead_source
      , b.form_type
      , LOWER(CONCAT(JSON_EXTRACT_SCALAR(form_keywords, '$.bill_to_email'), ' ', b.lead_source, ' ', created_date)) AS webform_lead_id

FROM    {{ source('wheelhouse_red_sox', 'forms2_detail') }} AS a
        INNER JOIN {{ ref('web_forms') }} AS b
            ON a.form_id = b.form_id
        LEFT OUTER JOIN {{ ref('lead_exclude') }} AS le
            ON JSON_EXTRACT_SCALAR(a.form_keywords, '$.bill_to_forename') = le.first_name
            AND JSON_EXTRACT_SCALAR(a.form_keywords, '$.bill_to_surname') = le.last_name
WHERE   le.first_name IS NULL 
  AND   le.last_name IS NULL

