{{
    config(
        cluster_by=['account_id']
    )
}}

WITH opportunity AS (
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT  salesforce_account_id
              , status
              , close_date
              , ROW_NUMBER() OVER (PARTITION BY salesforce_account_id ORDER BY close_date DESC) AS row_num
        FROM {{ ref('salesforce__opportunities__latest') }}
    )
    WHERE row_num = 1
)


SELECT  a.account_id
      , a.account_name
      , a.provenue_account_id
      , a.account_source
      , a.type
      , a.phone
      , a.sic_description
      , a.owner_id
      , a.datacom_key
      , a.description
      , a.number_of_employees
      , a.industry
      , a.parent_account_id
      , a.website
      , a.attributes_type
      , a.attributes_url
      , a.last_modified_by_id
      , a.last_modified_ts
      , a.created_by_id
      , a.imported_at_ts
      , a.is_deleted
      , billing_street
      , billing_city
      , billing_country
      , billing_state
      , billing_postal_code
      , shipping_street
      , shipping_city
      , shipping_country
      , shipping_state
      , shipping_postal_code
      , DATE_DIFF(CURRENT_DATE(), DATE(o.close_date), DAY) AS days_since_closed_lost 


FROM    {{ ref('salesforce__accounts__latest') }} AS a
        LEFT OUTER JOIN opportunity AS o 
            ON a.account_id = o.salesforce_account_id AND o.status = 'Closed Lost' AND o.salesforce_account_id IS NOT NULL
   
WHERE   is_deleted != True

  
