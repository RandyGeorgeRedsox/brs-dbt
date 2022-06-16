{{
    config(
        materialized="view"
      , cluster_by=['buyer_type_id']
    )
}}
SELECT  bt.buyer_type_id
      , bt.buyer_type_code
      , bt.buyer_type AS buyer_type_description
      , bg.buyer_type_group_code
      , bg.description AS buyer_type_group_description
      
FROM    {{ ref('tdc__buyer_type__base') }} AS bt        
        LEFT JOIN {{ ref('tdc__buyer_type_group__base') }}  AS bg       
            ON bg.buyer_type_group_id = bt.report_buyer_type_group_id  