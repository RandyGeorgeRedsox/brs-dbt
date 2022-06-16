SELECT  package_id
      , source_package_id
      , source_package_code
      , source_package_desc
      , source_package_type_code
      , source_package_type_desc
      , source_buyer_type_code
      , source_buyer_type_desc
      , source_buyer_type_group_code
      , source_buyer_type_group_desc
      , source_buyer_type_public_desc
      , approx_num_events
      , renewal_flag
      , group_flag
      , community_ticket_flag
      , flex_flag
      , package_type_desc
      , modified_date
      , team_nickname
FROM    {{ source('wheelhouse_red_sox', 'ticket_details_packages_tdc') }}