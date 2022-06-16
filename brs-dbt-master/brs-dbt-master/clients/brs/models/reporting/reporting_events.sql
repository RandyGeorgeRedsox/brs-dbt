{{
    config(
        materialized="view"
      , cluster_by=['event_id']
    )
}}
  SELECT  event_id
        , event_date
        , event_code 
        , event_name AS event_description
  FROM    {{ ref('tdc__event__base') }}
  WHERE   EXTRACT(YEAR FROM event_date) >= (SELECT MIN(year) FROM {{ ref('reference_years') }})
          AND (REGEXP_CONTAINS(event_code, '.*(RS|SD|LF|CA|GM|LC).*') --LC, life concert
               OR event_code LIKE 'RS%T%LFE')
          AND event_code NOT LIKE 'RS%VE' 
