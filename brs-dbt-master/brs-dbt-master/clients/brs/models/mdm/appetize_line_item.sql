{{
    config(
        cluster_by=['order_id']
    )
}}

SELECT  DISTINCT 
        CONCAT(app.order_id, '_', ig.row_id) AS order_row
      , app.order_id
      , ig.item_group.name AS item_category_name
      , ig.locationProductName AS item_brand
    --   , ig.item_group_3.ownerEntityType AS item_group_3_owner_entity_type
      , ig.item_group_3.name AS item_group_3_name
      , ig.item_group_3.id AS item_group_3_id
    --   , ig.item_group_2.ownerEntityType AS item_group_2_owner_entity_type
      , ig.item_group_2.name AS item_group_2_name
      , ig.item_group_2.id AS item_group_2_id
    --   , ig.item_group.ownerEntityType AS item_group_owner_entity_type
      , ig.item_group.name AS item_group_name
      , ig.item_group.id AS item_group_id
      , ig.id AS item_id
      , ig.quantity
      , ig.type
      , ig.totalAmount AS total_amount
      , ig.cost
      , ig.name
      , ig.departmentDescription AS department_description
      , ig.departmentId AS department_id
      , ig.tax
      , ig.refund_item_cost
      , ig.refund_item_quantity
      , (IFNULL(CAST(ig.refund_item_cost AS FLOAT64),0) * IFNULL(CAST(ig.refund_item_quantity AS FLOAT64),0)) AS total_refunds
      , ig.row_id
      , CAST(ig.inclusive_tax_amount AS FLOAT64) AS inclusive_tax_per_quantity
      , (IFNULL(CAST(ig.inclusive_tax_amount AS FLOAT64),0) * IFNULL(CAST(ig.quantity AS FLOAT64),0)) AS inclusive_tax_amount
      , CAST(ig.totalamount as FLOAT64) - (IFNULL(CAST(ig.inclusive_tax_amount AS FLOAT64),0) * IFNULL(CAST(ig.quantity AS FLOAT64),0)) as subtotal
    --   , bg.game_pk
    --   , bg.game_type
    --   , bg.game_nbr
    --   , bg.double_header
    --   , bg.makeup_game_flag
    --   , bg.game_time_local
    --   , bg.start_time
    --   , bg.game_elapsed_mins
    --   , bg.home_score
    --   , bg.away_team_code
    --   , bg.away_team
    --   , bg.away_score
    --   , bg.innings
    --   , bg.attendance
    --   , bg.temperature
    --   , bg.weather_condition
    --   , bg.game_time_home
    --   , "0052h000000kNA6AAM" AS created_by
FROM    {{ ref('appetize_latest') }} AS app
        LEFT OUTER JOIN UNNEST( items ) AS ig
        LEFT OUTER JOIN UNNEST(payments) AS p
        -- LEFT OUTER JOIN {{ ref('wheelhouse__baseball_game_master') }} bg
        --     ON DATE(app.create_time) = DATE(bg.game_time_home) 
        -- LEFT OUTER JOIN {{ ref('account') }} AS a
        --     ON app.customer.id = a.provenue.account_id
        -- LEFT OUTER JOIN {{ ref('contact') }} AS c
        --     ON a.provenue.account_id = c.provenue.account_id 
-- WHERE   bg.home_team = 'Red Sox'
-- AND     a.provenue.is_active = 1
-- AND     c.provenue.is_primary IS TRUE