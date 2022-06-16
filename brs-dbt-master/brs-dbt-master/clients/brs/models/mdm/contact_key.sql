SELECT  DISTINCT _composite_key
      , provenue.contact_id AS pv_contact_id
      , salesforce.contact_id AS sf_contact_id
      , wheelhouse.email AS wheelhouse_email

FROM  {{ ref('contact') }}