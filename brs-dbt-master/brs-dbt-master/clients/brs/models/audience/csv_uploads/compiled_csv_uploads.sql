{{
  config(
    materialized="table",
    unique_key="email",
    cluster_by=["email"]
  )
}}

WITH union_cte AS (
    SELECT  DISTINCT LOWER(email) AS email
          , 'Last Minute April May Buyers' AS audience
         
    FROM    {{ ref('fb_campaign0329') }}

    UNION ALL

    SELECT  LOWER(email)
          , '202106_nyy' AS audience
       
    FROM    {{ ref('nyy_targeted')}}

    UNION ALL

    SELECT  LOWER(email)
          , '202106_pedroia' AS audience
        
    FROM    {{ ref('pedroia_targeted')}}

    UNION ALL

    SELECT  LOWER(email)
          , '2021_season_ticket_holder_2022_reservation' AS audience

    FROM    {{ ref('2021_2022_sth_reservation') }}

)

SELECT  email
      , ARRAY_AGG(audience) AS csv_audiences
     
FROM    union_CTE
GROUP BY
        email
      