{{
    config(
        cluster_by=['flywheel_id']
    )
}}

WITH asset AS (
      SELECT  * EXCEPT(row_num)
      FROM ( SELECT child_member_id
                  , membership_type
                  , salesforce.contact_id 
                  , ROW_NUMBER() OVER (PARTITION BY CONCAT(child_member_id, membership_type) ORDER BY email DESC) AS row_num
             FROM   {{ ref('fortress__kidnation__latest') }} kd
             LEFT OUTER JOIN {{ ref('contact') }} c
                  ON kd.email = c.salesforce.email
            )
      WHERE  row_num = 1
)
,    member_asset AS (
      SELECT  child_member_id
             , ARRAY_AGG(
                  STRUCT(
                      child_member_id AS member
                    , m.membership
                    , age_range
                    , m.handedness
                    , m.rookie_delivery_type
                    , contact_id AS guardian
                    , CASE WHEN m.membership_type IN ('Rookie', 'Rookie (Mailed) $5.95', 'Rookie (Free Pick-Up) ($0.00)') THEN '01t4P000008iYB7QAM'
                           ELSE '01t4P000008iYB6QAM' END AS product_id
                        )
                        ) AS asset
      FROM asset AS a
      LEFT OUTER JOIN {{ ref('asset_membership') }} AS m
            ON a.membership_type = m.membership_type
      GROUP BY child_member_id
)
, member AS (
      SELECT  kd.shipping_country
            , kd.shipping_city
            , kd.shipping_street
            , kd.favorite_ballpark_snack
            , CASE WHEN kd.favorite_school_subject = 'English literature'
                   THEN 'English' ELSE favorite_school_subject END AS favorite_school_subject
            , TO_HEX(SHA1(CONCAT(kd.child_member_id, '_', IFNULL(c.salesforce.contact_id, ''), '_', IFNULL(kd.child_barcode, '')))) AS flywheel_id
            , m.salesforce_member_id
            , kd.num_assigned_tickets
            , kd.ticket_redemption_code
            , kd.date_upgraded
            , kd.activation_date
            , kd.shipping_state
            , IF(kd.upgraded = 'Yes', TRUE, FALSE) AS upgraded
            , kd.shipping_zip_code
            , CASE WHEN kd.child_gender = 'M' THEN 'Male'
                   WHEN kd.child_gender = 'F' THEN 'Female' END AS child_gender
            , kd.child_patron_id
            , kd.child_barcode
            , kd.child_surname
            , CASE WHEN kd.membership_type = 'Rookie' THEN 'Rookie'
                   WHEN kd.membership_type = 'Rookie (Mailed) $5.95' THEN 'Rookie - Mailed'
                   WHEN kd.membership_type = 'Rookie (Free Pick-Up) ($0.00)' THEN 'Rookie - Free Pick-Up'
                   WHEN kd.membership_type = 'All-Star Kit (Age 7-10), left handed ($54.95)' THEN 'All-Star - 7-10 - LH'
                   WHEN kd.membership_type = 'All Star Kit (Age 7-10), right handed ($54.95)' THEN 'All-Star - 7-10 - RH'
                   WHEN kd.membership_type = 'All-Star Kit (Age 11-14), left handed ($54.95)' THEN 'All-Star - 11-14 - LH'
                   WHEN kd.membership_type = 'All-Star Kit (Age 11-14), right handed ($54.95)' THEN 'All-Star - 11-14 - RH'
                   WHEN kd.membership_type = 'All-Star Kit (Age 6 & Under), left handed ($54.95)' THEN 'All-Star - 6&U - LH'
                   WHEN kd.membership_type = 'All-Star Kit (Age 6 & Under), right handed ($54.95)' THEN 'All-Star - 6&U - RH'
                   ELSE '' END AS membership_type
            , kd.child_creation_date
            , kd.child_date_of_birth
            , kd.child_member_id
            , ma.asset
            , kd.child_card_number
            , kd.child_firstname
            , kd.email
            , c.salesforce.contact_id

      FROM    {{ ref('fortress__kidnation__latest') }} kd
              LEFT OUTER JOIN {{ ref('contact') }} c
                  ON kd.email = c.salesforce.email
              LEFT OUTER JOIN member_asset ma
                  ON kd.child_member_id = ma.child_member_id
              LEFT OUTER JOIN {{ ref('salesforce__members__latest') }}  AS m
                  ON kd.child_member_id = m.fortress_member_id
)

SELECT *
FROM   member m
       LEFT OUTER JOIN {{ ref('member_exclude') }} me
            USING (flywheel_id)
WHERE  me.flywheel_id IS NULL
