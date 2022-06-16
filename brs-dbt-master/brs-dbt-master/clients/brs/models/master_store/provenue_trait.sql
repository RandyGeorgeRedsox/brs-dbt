WITH cte_latest_email_per_contact AS (
-- for every contact, give me the latest email
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT patron_contact_id 
           , LOWER(email) AS email 
           , primary
           , last_updated_date
           , ROW_NUMBER() OVER(PARTITION BY patron_contact_id ORDER BY last_updated_date DESC) AS row_num
      FROM   {{ source('wheelhouse_red_sox_ticketing', 'patron_contact_email')}}
      WHERE primary = 1
    )
    WHERE row_num = 1
)

-- for every email, give me the latest contact
, cte_latest_contact_per_email AS (
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT patron_contact_id 
           , email
           , primary
           , last_updated_date
           , ROW_NUMBER() OVER(PARTITION BY email ORDER BY last_updated_date DESC) AS row_num
      FROM   cte_latest_email_per_contact
      WHERE primary = 1
    )
    WHERE row_num = 1
)
, patron_email AS (
SELECT  pc.patron_contact_id
      , pc.patron_account_id
      , pc.primary 
      , pc.first_name
      , pc.last_name
      , pc.middle_name
      , pc.formatted_name
      , pc.formal_salutation
      , e.email
FROM   {{ source('wheelhouse_red_sox_ticketing', 'patron_contact')}} AS pc    
      LEFT OUTER JOIN cte_latest_contact_per_email AS e
          ON pc.patron_contact_id = e.patron_contact_id
WHERE  pc.primary = 1
)

SELECT DISTINCT pe.email
     , pt.patron_account_id
     , CONCAT(CAST(pt.patron_account_id AS STRING), '_', t.description) AS primary_key
     , tg.trait_group_code
     , tg.description trait_group
     , t.description trait
     , CONCAT(IFNULL(cast(BOOLEAN_DATA as string),'')
        , IFNULL(cast(CURRENCY_DATA as string),'')
        , IFNULL(cast(DATE_DATA as string),'')
        , IFNULL(cast(INTEGER_DATA as string),'')
        , IFNULL(cast (MEMO_DATA as string),'')
        , IFNULL(cast(STRING_DATA as string),'')) trait_value
FROM {{ source('wheelhouse_red_sox_ticketing', 'trait')}} t 
    INNER JOIN {{ source('wheelhouse_red_sox_ticketing', 'trait_group')}} tg 
      ON t.trait_group_id = tg.trait_group_id
    INNER JOIN {{ source('wheelhouse_red_sox_ticketing', 'patron_trait')}} pt 
      ON t.trait_id = pt.trait_id
    INNER JOIN patron_email AS pe 
      ON pt.patron_account_id = pe.patron_account_id
-- WHERE pt.patron_account_id = 4117907
