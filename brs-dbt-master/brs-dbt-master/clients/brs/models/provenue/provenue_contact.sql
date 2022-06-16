WITH latest_email_by_contact_id_cte AS (
    -- from every contact_id, give me the latest email
    SELECT  contact_id
          , email
          , is_primary
          , last_updated_ts
          , ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY last_updated_ts DESC) AS row_num
    FROM    {{ ref('tdc__patron_contact_email__base') }}
    WHERE   is_primary IS TRUE
)

,  cte_address AS (
    SELECT  contact_id
          , ARRAY_AGG(
                STRUCT(
                    street_address AS street_address_line_1
                  , street_address_line_2
                  , city
                  , country
                  , state
                  , postal_code_display
                  , address_type
                  , is_primary_billing
                  , is_primary
                )
            ) AS address
    FROM  {{ ref('tdc__patron_contact_address__base') }}
    WHERE is_primary IS TRUE
    GROUP BY contact_id
)

,  cte_phone AS (
    SELECT  contact_id
          , ARRAY_AGG(
                STRUCT(
                    country_code
                  , phone_number
                  , extension
                  , patron_phone_type_code AS type_code
                  , is_mobile
                  , is_primary
                )
            ) AS phone
    FROM  {{ ref('tdc__patron_contact_phone__base') }}
    WHERE is_primary IS TRUE
    GROUP BY contact_id

)

,  cte_designation AS (
    SELECT  email
          , SUM(CASE WHEN designation = 'Likely Broker' THEN 1 ELSE 0 END) AS is_designated_likely_broker
          , SUM(CASE WHEN designation = 'Sponsor' THEN 1 ELSE 0 END) AS is_designated_sponsor
          , SUM(CASE WHEN designation = 'MLB' THEN 1 ELSE 0 END) AS is_designated_mlb
          , SUM(CASE WHEN designation = 'Trade' THEN 1 ELSE 0 END) AS is_designated_trade
          , SUM(CASE WHEN designation = 'Internal' THEN 1 ELSE 0 END) AS is_designated_internal
          
    FROM  {{ ref('designation_email') }}
    GROUP BY email

)

SELECT  c.contact_id
      , c.account_name AS account_id
      , c.first_name
      , c.last_name
      , c.birthday
      , c.title
      , c.is_primary
      , b.email
      , a.address
      , p.phone
      , IF(d.is_designated_likely_broker > 0, TRUE, FALSE) AS is_designated_likely_broker
      , IF(d.is_designated_sponsor > 0, TRUE, FALSE) AS is_designated_sponsor
      , IF(d.is_designated_mlb > 0, TRUE, FALSE) AS is_designated_mlb
      , IF(d.is_designated_trade > 0, TRUE, FALSE) AS is_designated_trade
      , IF(d.is_designated_internal > 0, TRUE, FALSE) AS is_designated_internal
      , csv.csv_audiences
      , ROW_NUMBER() OVER (PARTITION BY b.email ORDER BY b.last_updated_ts DESC) AS email_recency_rank
      , ROW_NUMBER() OVER (PARTITION BY b.email, c.account_name ORDER BY c.is_primary DESC, b.last_updated_ts DESC) AS primary_rank

FROM    {{ ref('tdc__patron_contact__base') }} AS c

        INNER JOIN latest_email_by_contact_id_cte AS b
            ON c.contact_id = b.contact_id
            AND b.row_num = 1

        LEFT OUTER JOIN cte_address AS a
            ON c.contact_id = a.contact_id

        LEFT OUTER JOIN cte_phone AS p
            ON c.contact_id = p.contact_id

        LEFT OUTER JOIN cte_designation d
            ON b.email = d.email

        LEFT OUTER JOIN {{ ref('compiled_csv_uploads') }} AS csv
            ON b.email = csv.email

WHERE  c.account_name != 6562187