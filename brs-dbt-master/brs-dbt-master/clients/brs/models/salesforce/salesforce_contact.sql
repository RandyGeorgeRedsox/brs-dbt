SELECT  contact_id
      , contact_name
      , account_id
      , email
      , title
      , phone
      , provenue_contact_id

FROM    {{ ref('salesforce__contacts__latest') }}

WHERE   email IS NOT NULL
