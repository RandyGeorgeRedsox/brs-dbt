{{
    config(
        cluster_by=['_composite_key']
    )
}}

WITH cte_address AS (
          SELECT      
            CONCAT(IFNULL(sf.account_id,''), '_', IFNULL(CAST(pv.account_id AS STRING),'')) AS _composite_key
          , ARRAY_AGG(
                STRUCT(
            COALESCE(billing_street, CONCAT(IFNULL(pv_ad.street_address_line_1,''), ' ', IFNULL(pv_ad.street_address_line_2,''))) AS billing_street
          , COALESCE(billing_city, pv_ad.city) AS billing_city
          , COALESCE(billing_state, pv_ad.state) AS billing_state
          , COALESCE(billing_country, pv_ad.country) AS billing_country
          , COALESCE(billing_postal_code, pv_ad.postal_code_display) AS billing_postal_code
          ) 
    ) AS billing_address
          , ARRAY_AGG(
                STRUCT(
            COALESCE(shipping_street, billing_street, CONCAT(IFNULL(pv_ad.street_address_line_1,''), ' ', IFNULL(pv_ad.street_address_line_2,''))) AS shipping_street
          , COALESCE(shipping_city, billing_city, pv_ad.city) AS shipping_city
          , COALESCE(shipping_state, billing_state, pv_ad.state) AS shipping_state
          , COALESCE(shipping_country, billing_country, pv_ad.country) AS shipping_country
          , COALESCE(shipping_postal_code, billing_postal_code, pv_ad.postal_code_display) AS shipping_postal_code
          ) 
    ) AS shipping_address
    FROM    {{ ref('provenue_account') }} AS pv
            CROSS JOIN UNNEST( pv.address ) pv_ad
            FULL OUTER JOIN {{ ref('salesforce_account') }} AS sf
            ON pv.account_id = sf.provenue_account_id
    GROUP BY CONCAT(IFNULL(sf.account_id,''), '_', IFNULL(CAST(pv.account_id AS STRING),'')) 
    )


SELECT  
        COALESCE(pv.account_name, sf.account_name) AS account_name
      , COALESCE(sf.account_source, pv.account_source) AS account_source
      , TO_HEX(SHA1(CONCAT(IFNULL(sf.account_id,''), '_', IFNULL(CAST(pv.account_id AS STRING),'')))) AS _composite_key
      -- , COALESCE(sf.owner_id, '0052h000000kNA6AAM') AS account_owner  -- Delete if not required field
      
      , STRUCT(
            pv.account_id
          , pv.account_name
          , pv.created_ts
          , pv.account_type_code
          , pv.service_rep_id
          , pv.is_active
          
          -- Calculated Fields --
          , pv.current_red_sox_season_ticket_holder 
          , pv.lapsed_red_sox_season_ticket_holder 
          , pv.red_sox_season_ticket_waitlist_member 
          , pv.individual_ticket_buyer_2019
          , pv.individual_ticket_buyer_2020
          , pv.individual_ticket_buyer_2021
          , pv.individual_ticket_buyer_2022 
          , pv.current_red_sox_parital_plan_buyer 
          , pv.lapsed_red_sox_partial_plan_buyer 
          , pv.current_red_sox_group_buyer 
          , pv.lapsed_red_sox_group_buyer 
          , pv.red_sox_suite_renter
          , pv.has_reserved_season_tickets
          , pv.is_merged
          , pv.is_merged_target

          -- Aggregate Fields --
          , pv.total_spend
          , pv.package_spend
          , pv.group_spend
          , pv.single_spend
          , pv.last_year_spend
          , pv.plan_event_total_spend
          
          , STRUCT(
                pv.contact_id
              , pv.phone
              , pv.address
              , pv.email
            ) AS primary_contact
        ) AS provenue

      , STRUCT(
            sf.account_id
          , sf.account_name
          , sf.parent_account_id
          , sf.datacom_key
          , sf.description
          , sf.number_of_employees
          , sf.phone
          , sf.industry
          , sf.sic_description
          , sf.website
          , sf.created_by_id
          , sf.last_modified_by_id
          , ad.billing_address
          , ad.shipping_address
          , sf.days_since_closed_lost
        ) AS salesforce

      

FROM    {{ ref('provenue_account') }} AS pv

        FULL OUTER JOIN {{ ref('salesforce_account') }} AS sf
            ON pv.account_id = sf.provenue_account_id
        LEFT OUTER JOIN cte_address AS ad
            ON CONCAT(IFNULL(sf.account_id,''), '_', IFNULL(CAST(pv.account_id AS STRING),'')) = ad._composite_key

/*
eric@flywheelsoftware.com - notes:
- any fields not combined from multiple sources go in a source specific struct
- fields that are combined have logic matching the data dictionary transformation field (https://docs.google.com/spreadsheets/d/1Elqq6xHi6cAoYdzNvWkxUR2c8LixnoXAbIq2Z2JRPEs/edit?usp=sharing)
*/
