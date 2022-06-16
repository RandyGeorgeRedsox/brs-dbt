{{
    config(
        materialized= 'ephemeral'
    )
}}

SELECT  data_source
      , order_date
      , order_number
      , product_id
      , sku
      , product_name
      , CONCAT(order_number, product_id, sku, demand_quantity, selling_unit_price) AS primary_key
    --   , product_short_description
    --   , product_category
    --   , product_class
    --   , brand_name
    --   , image_url_50x50
    --   , image_url_500x500
    --   , destination_url
      , demand_quantity
      , selling_unit_price
    --   , team_id
    --   , team_name
    --   , email_id
      , email
    --   , first_name
    --   , last_name
    --   , dayphone
    --   , eveningphone
    --   , mobilephone
    --   , address_1
    --   , address_2
    --   , address_city
    --   , address_state
    --   , address_zip
    --   , address_zip_plus4
    --   , address_country
      , dw_add_tsp
      , batch_date
      , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'shop_sales') }}