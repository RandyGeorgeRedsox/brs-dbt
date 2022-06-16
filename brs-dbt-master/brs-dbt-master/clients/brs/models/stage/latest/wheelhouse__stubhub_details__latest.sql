{{
    config(
        cluster_by=['buyer_email']
    )
}}


SELECT  * EXCEPT(row_num)
FROM    ( SELECT  *
                , ROW_NUMBER() OVER (PARTITION BY buyer_email, seller_email, confirmation_date, seat_section, seats, seat_row, event_date, sale_amount ORDER BY batch_date DESC) as row_num
          FROM    {{ ref('wheelhouse__stubhub_details__base') }}
          )
WHERE   row_num = 1
