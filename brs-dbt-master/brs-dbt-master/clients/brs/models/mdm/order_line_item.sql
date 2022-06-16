SELECT    order_line_item_id
        , order_id
        , provenue_account_id
        , description
        , event
        , package_id
        , transaction_type_code


FROM      {{ ref('provenue_order_line_item') }} 